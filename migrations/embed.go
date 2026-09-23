// Package migrations embeds NEBULA's SQL migrations so every binary that needs
// them carries them, and no deployment depends on a separate file being present.
//
// This is what makes `nebula-migrate` a single static binary, and what lets a
// service assert at startup that the database is at the schema version it was
// built against (docs/data-model.md §10).
package migrations

import "embed"

// FS holds the migration files, named NNNNNN_name.(up|down).sql.
//
//go:embed *.sql
var FS embed.FS
