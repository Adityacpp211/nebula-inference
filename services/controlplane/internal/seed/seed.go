// Package seed creates the development organization, user and API key.
//
// This is a DECLARED DEVELOPMENT STUB in the sense the specification requires: it
// runs only when NEBULA_DEV_SEED=true, configuration validation refuses that flag
// in production, and this package refuses again at its own entry point rather than
// trusting that check. The double refusal is deliberate — a seeder that runs in
// production writes a credential whose plaintext appeared in a log.
//
// It is idempotent: the point is `docker compose up` followed by a working curl,
// repeatedly, without accumulating organizations.
package seed

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// Development fixtures. Fixed slug and email so a developer's shell history keeps
// working across rebuilds; the API key is freshly generated every time, because a
// hard-coded key would eventually be committed to something.
const (
	OrgSlug   = "dev"
	OrgName   = "Development"
	Namespace = "nebula-dev"
	// A dot is required: ck_users__email_shape refuses an address with no domain
	// part, so "dev@localhost" is rejected by the schema.
	UserEmail = "dev@nebula.local"
	UserName  = "Development Owner"
	KeyName   = "development seed key"
)

// advisoryLockKey serialises seeding across replicas.
//
// Two control-plane processes starting at once would otherwise race on the unique
// slug and one would fail its startup. An advisory lock is the right tool: it is
// released automatically when the transaction ends, including when the process
// dies mid-seed.
const advisoryLockKey int64 = 0x4e45425541530001 // "NEBUAS" + 1

// Result reports what the seeder did.
type Result struct {
	OrgID  string
	UserID string
	KeyID  string
	Prefix string
	// Plaintext is set only when a key was created in this run. It cannot be
	// recovered later: the server stores an HMAC.
	Plaintext string
	Created   bool
}

// Run seeds development fixtures, returning what exists afterwards.
func Run(ctx context.Context, cfg *config.Config, st *store.Store, hasher *auth.Hasher,
	logger *slog.Logger) (*Result, error) {
	if !cfg.Dev.Seed {
		return nil, nil
	}
	if cfg.Env.IsProduction() {
		// Refused here as well as in configuration validation. Defence in depth for
		// the one code path that writes a credential and logs its plaintext.
		return nil, errors.New("refusing to seed development fixtures in production")
	}

	var result Result
	err := db.InTx(ctx, st.Pool(), func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, advisoryLockKey); err != nil {
			return fmt.Errorf("taking the seed advisory lock: %w", err)
		}

		org, err := ensureOrg(ctx, tx, st)
		if err != nil {
			return err
		}
		result.OrgID = org.ID.String()

		// Row-level security applies to the tables below, so the session has to name
		// the tenant it is writing before touching them.
		if err := db.SetSessionOrg(ctx, tx, org.ID); err != nil {
			return err
		}

		user, err := ensureUser(ctx, tx, st, org.ID)
		if err != nil {
			return err
		}
		result.UserID = user.ID.String()

		existing, err := findSeedKey(ctx, tx, st, org.ID)
		if err != nil {
			return err
		}
		if existing != nil {
			result.KeyID = existing.ID.String()
			result.Prefix = existing.Prefix
			return nil
		}

		generated, err := hasher.Generate()
		if err != nil {
			return fmt.Errorf("generating the seed API key: %w", err)
		}
		id, err := db.NewID()
		if err != nil {
			return err
		}
		key := &models.APIKey{
			ID:     id,
			OrgID:  org.ID,
			UserID: &user.ID,
			Name:   KeyName,
			Prefix: generated.Prefix,
			// The seed key carries every scope because it stands in for the operator
			// during development. That is exactly why it must never exist in
			// production, and why the flag that creates it is refused there.
			Scopes:   auth.Strings(auth.AllScopes()),
			Priority: models.PriorityNormal,
			KeyHash:  generated.Hash,
		}
		if err := st.APIKeys.Create(ctx, tx, key); err != nil {
			return fmt.Errorf("creating the seed API key: %w", err)
		}

		result.KeyID = key.ID.String()
		result.Prefix = key.Prefix
		result.Plaintext = generated.Plaintext
		result.Created = true

		entry := store.Entry{
			OrgID:        org.ID,
			ActorType:    models.ActorSystem,
			Action:       store.ActionAPIKeyCreate,
			ResourceType: store.ResourceAPIKey,
			ResourceID:   &key.ID,
			After: map[string]any{
				"name":   key.Name,
				"prefix": key.Prefix,
				"scopes": key.Scopes,
				"source": "development seed",
			},
		}
		return st.Audit.Append(ctx, tx, entry)
	})
	if err != nil {
		return nil, err
	}

	if result.Created {
		// The plaintext is logged exactly once, here, and only in development. A
		// developer needs it and there is nowhere else to put it; the same line in
		// production would be a leaked credential, which is what the two refusals
		// above are for.
		logger.WarnContext(ctx, "development seed: API key created; it will not be shown again",
			slog.String("api_key", result.Plaintext),
			slog.String("prefix", result.Prefix),
			slog.String("org_slug", OrgSlug))
	} else {
		logger.InfoContext(ctx, "development seed: fixtures already present",
			slog.String("org_slug", OrgSlug),
			slog.String("key_prefix", result.Prefix),
			slog.String("hint", "revoke the existing key and restart to issue a new one"))
	}
	return &result, nil
}

func ensureOrg(ctx context.Context, tx pgx.Tx, st *store.Store) (*models.Organization, error) {
	org, err := st.Organizations.GetBySlug(ctx, tx, OrgSlug)
	switch {
	case err == nil:
		return org, nil
	case !errors.Is(err, store.ErrNotFound):
		return nil, fmt.Errorf("looking up the development organization: %w", err)
	}

	id, err := db.NewID()
	if err != nil {
		return nil, err
	}
	org = &models.Organization{
		ID:               id,
		Slug:             OrgSlug,
		Name:             OrgName,
		DefaultNamespace: Namespace,
	}
	if err := st.Organizations.Create(ctx, tx, org); err != nil {
		return nil, fmt.Errorf("creating the development organization: %w", err)
	}
	return org, nil
}

func ensureUser(ctx context.Context, tx pgx.Tx, st *store.Store, orgID uuid.UUID) (*models.User, error) {
	user, err := st.Users.GetByEmail(ctx, tx, UserEmail)
	switch {
	case err == nil:
		return user, nil
	case !errors.Is(err, store.ErrNotFound):
		return nil, fmt.Errorf("looking up the development user: %w", err)
	}

	id, err := db.NewID()
	if err != nil {
		return nil, err
	}
	name := UserName
	user = &models.User{
		ID:    id,
		OrgID: orgID,
		Email: UserEmail,
		Name:  &name,
		Role:  models.RoleOwner,
		// No password: the seed user exists to own the seed key, and a development
		// password that works is a development password that reaches staging.
	}
	if err := st.Users.Create(ctx, tx, user); err != nil {
		return nil, fmt.Errorf("creating the development user: %w", err)
	}
	return user, nil
}

// findSeedKey returns the live seed key, if one exists.
//
// Matched by name and by not being revoked, so revoking the key and restarting
// issues a new one — which is how a developer who lost the plaintext recovers,
// without a flag for it.
func findSeedKey(ctx context.Context, tx pgx.Tx, st *store.Store, orgID uuid.UUID) (*models.APIKey, error) {
	keys, err := st.APIKeys.List(ctx, tx, orgID, store.NewPage(store.MaxPageLimit, nil))
	if err != nil {
		return nil, fmt.Errorf("listing API keys: %w", err)
	}
	for _, k := range keys {
		if k.Name == KeyName && k.RevokedAt == nil {
			return k, nil
		}
	}
	return nil, nil
}
