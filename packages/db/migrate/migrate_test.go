package migrate_test

import (
	"bytes"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/adityasatwar321/nebula/migrations"
	"github.com/adityasatwar321/nebula/packages/db/migrate"
)

func fsWith(files map[string]string) fstest.MapFS {
	m := fstest.MapFS{}
	for name, body := range files {
		m[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return m
}

func TestLoadOrdersByVersion(t *testing.T) {
	t.Parallel()

	got, err := migrate.Load(fsWith(map[string]string{
		"000003_c.up.sql": "SELECT 3;", "000003_c.down.sql": "SELECT -3;",
		"000001_a.up.sql": "SELECT 1;", "000001_a.down.sql": "SELECT -1;",
		"000002_b.up.sql": "SELECT 2;", "000002_b.down.sql": "SELECT -2;",
	}))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("loaded %d migrations, want 3", len(got))
	}
	for i, want := range []struct {
		version int64
		name    string
	}{{1, "a"}, {2, "b"}, {3, "c"}} {
		if got[i].Version != want.version || got[i].Name != want.name {
			t.Errorf("migration %d = %06d_%s, want %06d_%s", i, got[i].Version, got[i].Name, want.version, want.name)
		}
	}
	if len(got[0].UpChecksum) != 32 {
		t.Errorf("checksum length = %d, want 32 (sha256)", len(got[0].UpChecksum))
	}
}

// A down migration is mandatory. It forces the author to think about
// reversibility and lets local development rewind (docs/data-model.md §10).
func TestLoadRejectsMalformedSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		files map[string]string
		want  string
	}{
		{
			name:  "missing down",
			files: map[string]string{"000001_a.up.sql": "SELECT 1;"},
			want:  "no down migration",
		},
		{
			name:  "missing up",
			files: map[string]string{"000001_a.down.sql": "SELECT 1;"},
			want:  "no up migration",
		},
		{
			name:  "bad filename",
			files: map[string]string{"1_a.up.sql": "SELECT 1;"},
			want:  "does not match",
		},
		{
			name: "conflicting names for one version",
			files: map[string]string{
				"000001_a.up.sql": "SELECT 1;", "000001_a.down.sql": "SELECT -1;",
				"000001_b.up.sql": "SELECT 2;", "000001_b.down.sql": "SELECT -2;",
			},
			want: "conflicting names",
		},
		{
			name:  "version zero reserved",
			files: map[string]string{"000000_a.up.sql": "SELECT 1;", "000000_a.down.sql": "SELECT -1;"},
			want:  "version 0 is reserved",
		},
		{
			name:  "uppercase in name",
			files: map[string]string{"000001_BadName.up.sql": "x", "000001_BadName.down.sql": "x"},
			want:  "does not match",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := migrate.Load(fsWith(tt.files))
			if err == nil {
				t.Fatalf("Load() succeeded, want an error mentioning %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestLoadDetectsNoTransactionDirective(t *testing.T) {
	t.Parallel()

	got, err := migrate.Load(fsWith(map[string]string{
		"000001_plain.up.sql":   "CREATE TABLE t (id int);",
		"000001_plain.down.sql": "DROP TABLE t;",
		"000002_concurrent.up.sql": "-- nebula:no-transaction\n" +
			"CREATE INDEX CONCURRENTLY ix_t ON t (id);",
		"000002_concurrent.down.sql": "DROP INDEX ix_t;",
	}))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got[0].NoTransaction {
		t.Error("plain migration was marked no-transaction")
	}
	if !got[1].NoTransaction {
		t.Error("migration carrying the directive was not marked no-transaction")
	}
}

// Changing a migration's content must change its checksum: that is what makes
// drift detection work.
func TestChecksumChangesWithContent(t *testing.T) {
	t.Parallel()

	a, err := migrate.Load(fsWith(map[string]string{
		"000001_a.up.sql": "SELECT 1;", "000001_a.down.sql": "SELECT -1;",
	}))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	b, err := migrate.Load(fsWith(map[string]string{
		"000001_a.up.sql": "SELECT 2;", "000001_a.down.sql": "SELECT -1;",
	}))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if bytes.Equal(a[0].UpChecksum, b[0].UpChecksum) {
		t.Error("checksum did not change when the migration content changed")
	}

	// ...and must be stable for identical content.
	c, _ := migrate.Load(fsWith(map[string]string{
		"000001_a.up.sql": "SELECT 1;", "000001_a.down.sql": "SELECT -1;",
	}))
	if !bytes.Equal(a[0].UpChecksum, c[0].UpChecksum) {
		t.Error("checksum is not stable for identical content")
	}
}

// The real migration set must load, be contiguous from 1, and have a down
// migration for every up. This runs on every build, so a badly named or
// half-written migration fails fast.
func TestEmbeddedMigrationsAreWellFormed(t *testing.T) {
	t.Parallel()

	got, err := migrate.Load(migrations.FS)
	if err != nil {
		t.Fatalf("the embedded migrations do not load: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("no embedded migrations found")
	}

	for i, m := range got {
		if want := int64(i + 1); m.Version != want {
			t.Errorf("migration %d has version %d; versions must be contiguous from 1", i, m.Version)
		}
		if strings.TrimSpace(m.Up) == "" {
			t.Errorf("%06d_%s has an empty up migration", m.Version, m.Name)
		}
		if strings.TrimSpace(m.Down) == "" {
			t.Errorf("%06d_%s has an empty down migration", m.Version, m.Name)
		}
		if m.NoTransaction {
			// Such a file must hold exactly one statement, because PostgreSQL wraps
			// a multi-statement simple query in an implicit transaction anyway.
			if strings.Count(m.Up, ";") > 1 {
				t.Errorf("%06d_%s opts out of the transaction but has more than one statement",
					m.Version, m.Name)
			}
		}
	}
}
