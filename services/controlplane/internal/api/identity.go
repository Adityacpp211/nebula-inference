package api

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// ---------------------------------------------------------------------------
// users
// ---------------------------------------------------------------------------

func (a *API) listUsers(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	p, err := page(r)
	if err != nil {
		return err
	}
	rows, err := a.Store.Users.List(r.Context(), a.Store.Pool(), ident.OrgID, p)
	if err != nil {
		return storeError(err, "user")
	}
	out := make([]userResponse, 0, len(rows))
	for _, u := range rows {
		out = append(out, newUserResponse(u))
	}
	a.write(w, r, http.StatusOK, newList(out, p.Limit, func(u userResponse) string {
		return u.ID.String()
	}))
	return nil
}

func (a *API) createUser(w http.ResponseWriter, r *http.Request) error {
	var req createUserRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}

	email := strings.ToLower(strings.TrimSpace(req.Email))
	if email == "" {
		return httpx.ErrInvalidRequest("email is required", "missing_field", "email")
	}
	if !emailPattern.MatchString(email) || len(email) > 320 {
		return httpx.ErrInvalidRequest("email is not a valid address", "invalid_email", "email")
	}
	if err := requireEnum(req.Role, "role", roleStrings()); err != nil {
		return err
	}

	// An admin may not create an owner: owner carries billing and organization
	// deletion, so promoting to it is an owner's decision. Without this rule the
	// role hierarchy is decorative, because any admin could mint an owner.
	role := models.UserRole(req.Role)
	ident := auth.MustFromContext(r.Context())
	if role == models.RoleOwner && ident.Role != models.RoleOwner {
		return forbidden(
			"only an owner may create another owner", "insufficient_role")
	}

	var passwordHash *string
	if req.Password != "" {
		hash, err := auth.HashPassword(req.Password)
		if err != nil {
			if strings.Contains(err.Error(), "characters") {
				return httpx.ErrInvalidRequest(err.Error(), "invalid_password", "password")
			}
			return httpx.ErrInternal(err)
		}
		passwordHash = &hash
	}

	id, err := db.NewID()
	if err != nil {
		return httpx.ErrInternal(err)
	}
	u := &models.User{
		ID:           id,
		OrgID:        ident.OrgID,
		Email:        email,
		Name:         req.Name,
		Role:         role,
		PasswordHash: passwordHash,
	}

	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		if err := a.Store.Users.Create(ctx, q, u); err != nil {
			return storeError(err, "user")
		}
		entry := auditFor(r, store.ActionUserCreate, store.ResourceUser)
		entry.ResourceID = &u.ID
		// The response shape is reused as the audit payload precisely because it
		// contains no password hash: an audit row is read by more people than a
		// response is.
		entry.After = newUserResponse(u)
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	a.write(w, r, http.StatusCreated, newUserResponse(u))
	return nil
}

func (a *API) getUser(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "user_id", "user")
	if err != nil {
		return err
	}
	u, err := a.Store.Users.Get(r.Context(), a.Store.Pool(), ident.OrgID, id)
	if err != nil {
		return storeError(err, "user")
	}
	a.write(w, r, http.StatusOK, newUserResponse(u))
	return nil
}

func (a *API) updateUser(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "user_id", "user")
	if err != nil {
		return err
	}
	var req updateUserRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	if err := requireEnum(req.Role, "role", roleStrings()); err != nil {
		return err
	}
	role := models.UserRole(req.Role)

	var updated *models.User
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		before, err := a.Store.Users.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "user")
		}
		// Granting or removing owner is an owner's decision, in both directions:
		// otherwise an admin could demote every owner and take the organization.
		if (role == models.RoleOwner || before.Role == models.RoleOwner) &&
			ident.Role != models.RoleOwner {
			return forbidden("only an owner may change an owner's role", "insufficient_role")
		}
		if err := a.Store.Users.UpdateRole(ctx, q, ident.OrgID, id, role); err != nil {
			return storeError(err, "user")
		}
		after, err := a.Store.Users.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "user")
		}
		updated = after

		entry := auditFor(r, store.ActionUserUpdateRole, store.ResourceUser)
		entry.ResourceID = &id
		entry.Before = newUserResponse(before)
		entry.After = newUserResponse(after)
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	a.write(w, r, http.StatusOK, newUserResponse(updated))
	return nil
}

// deleteUser soft-deletes a user. The store refuses to remove the last owner,
// which is the difference between an organization that can still be administered
// and one that cannot.
func (a *API) deleteUser(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "user_id", "user")
	if err != nil {
		return err
	}

	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		before, err := a.Store.Users.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "user")
		}
		if before.Role == models.RoleOwner && ident.Role != models.RoleOwner {
			return forbidden("only an owner may remove an owner", "insufficient_role")
		}
		if err := a.Store.Users.SoftDelete(ctx, q, ident.OrgID, id); err != nil {
			return storeError(err, "user")
		}
		entry := auditFor(r, store.ActionUserDelete, store.ResourceUser)
		entry.ResourceID = &id
		entry.Before = newUserResponse(before)
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	w.WriteHeader(http.StatusNoContent)
	return nil
}

// ---------------------------------------------------------------------------
// API keys
// ---------------------------------------------------------------------------

func (a *API) listAPIKeys(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	p, err := page(r)
	if err != nil {
		return err
	}
	rows, err := a.Store.APIKeys.List(r.Context(), a.Store.Pool(), ident.OrgID, p)
	if err != nil {
		return storeError(err, "api key")
	}
	out := make([]apiKeyResponse, 0, len(rows))
	for _, k := range rows {
		out = append(out, newAPIKeyResponse(k))
	}
	a.write(w, r, http.StatusOK, newList(out, p.Limit, func(k apiKeyResponse) string {
		return k.ID.String()
	}))
	return nil
}

// createAPIKey mints a credential.
//
// Three rules are enforced here and nowhere else, because this is the only place a
// credential comes into existence:
//
//   - A caller cannot grant a scope its own credential does not carry. Otherwise
//     "admin" would be a privilege-escalation primitive: any admin key could mint
//     an unrestricted one, and scoping would be advisory.
//   - The plaintext is returned exactly once. The server keeps an HMAC and cannot
//     reproduce it.
//   - The scopes default from the owning user's role rather than to everything,
//     so a key created without thought is a limited key.
func (a *API) createAPIKey(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())

	var req createAPIKeyRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		return httpx.ErrInvalidRequest("name is required", "missing_field", "name")
	}
	if len(req.Name) > 128 {
		return httpx.ErrInvalidRequest("name must be at most 128 characters", "invalid_name", "name")
	}

	priority := models.PriorityNormal
	if req.Priority != "" {
		if err := requireEnum(req.Priority, "priority", []string{
			string(models.PriorityLow), string(models.PriorityNormal), string(models.PriorityHigh),
		}); err != nil {
			return err
		}
		priority = models.Priority(req.Priority)
	}
	if priority == models.PriorityHigh && !ident.IsAdmin() {
		return forbidden("only an admin credential may issue a HIGH priority key", "insufficient_scope")
	}

	if req.ExpiresAt != nil && !req.ExpiresAt.After(time.Now()) {
		return httpx.ErrInvalidRequest(
			"expires_at must be in the future", "invalid_expiry", "expires_at")
	}

	var scopes []auth.Scope
	if len(req.Scopes) > 0 {
		parsed, err := auth.ParseScopes(req.Scopes)
		if err != nil {
			return httpx.ErrInvalidRequest(err.Error(), "invalid_scope", "scopes")
		}
		scopes = parsed
	} else {
		scopes = auth.DefaultScopesForRole(ident.Role)
		if len(scopes) == 0 {
			return httpx.ErrInvalidRequest(
				"scopes is required: this credential has no role to default from",
				"missing_field", "scopes")
		}
	}

	// The granting credential's own scopes bound what it can grant.
	for _, s := range scopes {
		if !ident.Has(s) {
			return forbidden(
				"cannot grant the "+string(s)+" scope: the credential making this request does not carry it",
				"scope_escalation")
		}
	}

	generated, err := a.Hasher.Generate()
	if err != nil {
		return httpx.ErrInternal(err)
	}
	id, err := db.NewID()
	if err != nil {
		return httpx.ErrInternal(err)
	}

	k := &models.APIKey{
		ID:        id,
		OrgID:     ident.OrgID,
		UserID:    req.UserID,
		Name:      req.Name,
		Prefix:    generated.Prefix,
		KeyHash:   generated.Hash,
		Scopes:    auth.Strings(scopes),
		Priority:  priority,
		ExpiresAt: req.ExpiresAt,
	}

	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		// A key may only belong to a user in the same organization, checked rather
		// than left to the foreign key: the FK permits any user id, and a key
		// attributed to another tenant's user would mislabel every audit row it
		// produces.
		if req.UserID != nil {
			if _, err := a.Store.Users.Get(ctx, q, ident.OrgID, *req.UserID); err != nil {
				return storeError(err, "user")
			}
		}
		if err := a.Store.APIKeys.Create(ctx, q, k); err != nil {
			return storeError(err, "api key")
		}
		entry := auditFor(r, store.ActionAPIKeyCreate, store.ResourceAPIKey)
		entry.ResourceID = &k.ID
		entry.After = newAPIKeyResponse(k)
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	a.write(w, r, http.StatusCreated, createdAPIKeyResponse{
		apiKeyResponse: newAPIKeyResponse(k),
		Key:            generated.Plaintext,
		KeyNote: "this is the only time the key is shown: the server stores a keyed hash and " +
			"cannot reproduce it. Store it now; if it is lost, revoke this key and create another.",
	})
	return nil
}

// revokeAPIKey disables a credential.
//
// Revocation is idempotent in the store, and the local cache entry is dropped so
// the key stops working in this process immediately rather than after the cache
// TTL. The response names the revoked prefix in a header, which the gateway's admin
// proxy uses to evict the key from the shared Redis cache; other control-plane
// replicas and other gateway replicas' in-process caches still honour their own
// short TTLs, which is why those are small, deliberate numbers.
func (a *API) revokeAPIKey(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "key_id", "api key")
	if err != nil {
		return err
	}

	var prefix string
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		before, err := a.Store.APIKeys.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "api key")
		}
		prefix = before.Prefix
		if err := a.Store.APIKeys.Revoke(ctx, q, ident.OrgID, id); err != nil {
			return storeError(err, "api key")
		}
		entry := auditFor(r, store.ActionAPIKeyRevoke, store.ResourceAPIKey)
		entry.ResourceID = &id
		entry.Before = newAPIKeyResponse(before)
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}
	a.keys.forget(prefix)

	// Tells the gateway which cache entry to drop; see HeaderRevokedKeyPrefix.
	w.Header().Set(HeaderRevokedKeyPrefix, prefix)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// ---------------------------------------------------------------------------
// audit
// ---------------------------------------------------------------------------

func (a *API) listAuditLogs(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	q := r.URL.Query()

	f := store.AuditFilter{}
	if v := q.Get("action"); v != "" {
		f.Action = &v
	}
	if v := q.Get("resource_type"); v != "" {
		f.ResourceType = &v
	}
	if v := q.Get("resource_id"); v != "" {
		id, err := uuidParam(v, "resource_id")
		if err != nil {
			return err
		}
		f.ResourceID = &id
	}
	since, err := timeParam(q.Get("since"), "since")
	if err != nil {
		return err
	}
	f.Since = since
	until, err := timeParam(q.Get("until"), "until")
	if err != nil {
		return err
	}
	f.Until = until

	limit, err := pageLimit(r)
	if err != nil {
		return err
	}

	// The audit cursor is a pair, because the table is partitioned by created_at:
	// an id-only cursor would have to be searched for in every partition.
	var cursor *store.AuditCursor
	if raw := q.Get("cursor"); raw != "" {
		c, err := parseAuditCursor(raw)
		if err != nil {
			return err
		}
		cursor = c
	}

	rows, err := a.Store.Audit.List(r.Context(), a.Store.Pool(), ident.OrgID, f, cursor, limit)
	if err != nil {
		return storeError(err, "audit log")
	}

	out := make([]auditResponse, 0, len(rows))
	for _, e := range rows {
		out = append(out, newAuditResponse(e))
	}
	a.write(w, r, http.StatusOK, newList(out, limit, func(e auditResponse) string {
		return formatAuditCursor(e.CreatedAt, e.ID.String())
	}))
	return nil
}

// roleStrings lists the roles, for validation messages.
func roleStrings() []string {
	all := models.UserRoles()
	out := make([]string, 0, len(all))
	for _, r := range all {
		out = append(out, string(r))
	}
	return out
}
