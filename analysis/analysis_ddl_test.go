// analysis_ddl_test.go verifies DDL metadata in the analysis layer.
package analysis

import (
	"testing"
)

// TestAnalyzeSQL_DDL_DropTable validates DROP TABLE metadata including IF EXISTS, CASCADE, and multi-table.
func TestAnalyzeSQL_DDL_DropTable(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantCount  int
		wantObject string
		wantFlags  []string
		wantTables int
	}{
		{
			name:       "simple",
			sql:        "DROP TABLE users",
			wantCount:  1,
			wantObject: "users",
			wantTables: 1,
		},
		{
			name:       "IF EXISTS",
			sql:        "DROP TABLE IF EXISTS users",
			wantCount:  1,
			wantObject: "users",
			wantFlags:  []string{"IF_EXISTS"},
			wantTables: 1,
		},
		{
			name:       "CASCADE",
			sql:        "DROP TABLE users CASCADE",
			wantCount:  1,
			wantObject: "users",
			wantFlags:  []string{"CASCADE"},
			wantTables: 1,
		},
		{
			name:       "IF EXISTS CASCADE",
			sql:        "DROP TABLE IF EXISTS users CASCADE",
			wantCount:  1,
			wantObject: "users",
			wantFlags:  []string{"IF_EXISTS", "CASCADE"},
			wantTables: 1,
		},
		{
			name:       "schema-qualified",
			sql:        "DROP TABLE myschema.users",
			wantCount:  1,
			wantObject: "myschema.users",
			wantTables: 1,
		},
		{
			name:       "multiple tables",
			sql:        "DROP TABLE users, orders, products",
			wantCount:  3,
			wantTables: 3,
		},
		{
			name:       "RESTRICT",
			sql:        "DROP TABLE users RESTRICT",
			wantCount:  1,
			wantObject: "users",
			wantFlags:  []string{"RESTRICT"},
			wantTables: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := AnalyzeSQL(tc.sql)
			if err != nil {
				t.Fatalf("AnalyzeSQL failed: %v", err)
			}
			if res.Command != SQLCommandDDL {
				t.Fatalf("expected DDL command, got %s", res.Command)
			}
			if len(res.DDLActions) != tc.wantCount {
				t.Fatalf("expected %d DDL actions, got %d: %+v", tc.wantCount, len(res.DDLActions), res.DDLActions)
			}
			act := res.DDLActions[0]
			if act.Type != "DROP_TABLE" {
				t.Fatalf("expected DROP_TABLE, got %s", act.Type)
			}
			if tc.wantObject != "" && act.ObjectName != tc.wantObject {
				t.Fatalf("expected object %q, got %q", tc.wantObject, act.ObjectName)
			}
			for _, f := range tc.wantFlags {
				assertAnalysisFlag(t, act.Flags, f)
			}
			if len(res.Tables) != tc.wantTables {
				t.Fatalf("expected %d tables, got %d: %+v", tc.wantTables, len(res.Tables), res.Tables)
			}
		})
	}
}

// TestAnalyzeSQL_DDL_DropIndex verifies DROP INDEX flags like IF EXISTS and CONCURRENTLY.
func TestAnalyzeSQL_DDL_DropIndex(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantFlags []string
	}{
		{
			name: "simple",
			sql:  "DROP INDEX idx_users_email",
		},
		{
			name:      "IF EXISTS",
			sql:       "DROP INDEX IF EXISTS idx_users_email",
			wantFlags: []string{"IF_EXISTS"},
		},
		{
			name:      "CONCURRENTLY",
			sql:       "DROP INDEX CONCURRENTLY idx_users_email",
			wantFlags: []string{"CONCURRENTLY"},
		},
		{
			name:      "CONCURRENTLY IF EXISTS",
			sql:       "DROP INDEX CONCURRENTLY IF EXISTS idx_users_email",
			wantFlags: []string{"CONCURRENTLY", "IF_EXISTS"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := AnalyzeSQL(tc.sql)
			if err != nil {
				t.Fatalf("AnalyzeSQL failed: %v", err)
			}
			if res.Command != SQLCommandDDL {
				t.Fatalf("expected DDL command, got %s", res.Command)
			}
			if len(res.DDLActions) != 1 {
				t.Fatalf("expected 1 DDL action, got %d", len(res.DDLActions))
			}
			act := res.DDLActions[0]
			if act.Type != "DROP_INDEX" {
				t.Fatalf("expected DROP_INDEX, got %s", act.Type)
			}
			for _, f := range tc.wantFlags {
				assertAnalysisFlag(t, act.Flags, f)
			}
		})
	}
}

// TestAnalyzeSQL_DDL_CreateIndex checks CREATE INDEX variants including UNIQUE, CONCURRENTLY, and USING.
func TestAnalyzeSQL_DDL_CreateIndex(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantObject string
		wantCols   int
		wantFlags  []string
		wantIdx    string
		wantTable  string
	}{
		{
			name:       "simple",
			sql:        "CREATE INDEX idx_email ON users (email)",
			wantObject: "idx_email",
			wantCols:   1,
			wantTable:  "users",
		},
		{
			name:       "CONCURRENTLY",
			sql:        "CREATE INDEX CONCURRENTLY idx_email ON users (email)",
			wantObject: "idx_email",
			wantCols:   1,
			wantFlags:  []string{"CONCURRENTLY"},
			wantTable:  "users",
		},
		{
			name:       "UNIQUE",
			sql:        "CREATE UNIQUE INDEX idx_email ON users (email)",
			wantObject: "idx_email",
			wantCols:   1,
			wantFlags:  []string{"UNIQUE"},
			wantTable:  "users",
		},
		{
			name:       "UNIQUE CONCURRENTLY btree",
			sql:        "CREATE UNIQUE INDEX CONCURRENTLY idx_email ON users USING btree (email)",
			wantObject: "idx_email",
			wantCols:   1,
			wantFlags:  []string{"UNIQUE", "CONCURRENTLY"},
			wantIdx:    "btree",
			wantTable:  "users",
		},
		{
			name:       "USING gin",
			sql:        "CREATE INDEX idx_tags ON posts USING gin (tags)",
			wantObject: "idx_tags",
			wantCols:   1,
			wantIdx:    "gin",
			wantTable:  "posts",
		},
		{
			name:       "multi-column",
			sql:        "CREATE INDEX idx_compound ON users (last_name, first_name)",
			wantObject: "idx_compound",
			wantCols:   2,
			wantTable:  "users",
		},
		{
			name:       "IF NOT EXISTS",
			sql:        "CREATE INDEX IF NOT EXISTS idx_email ON users (email)",
			wantObject: "idx_email",
			wantCols:   1,
			wantFlags:  []string{"IF_NOT_EXISTS"},
			wantTable:  "users",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := AnalyzeSQL(tc.sql)
			if err != nil {
				t.Fatalf("AnalyzeSQL failed: %v", err)
			}
			if res.Command != SQLCommandDDL {
				t.Fatalf("expected DDL command, got %s", res.Command)
			}
			if len(res.DDLActions) != 1 {
				t.Fatalf("expected 1 DDL action, got %d", len(res.DDLActions))
			}
			act := res.DDLActions[0]
			if act.Type != "CREATE_INDEX" {
				t.Fatalf("expected CREATE_INDEX, got %s", act.Type)
			}
			if act.ObjectName != tc.wantObject {
				t.Fatalf("expected object %q, got %q", tc.wantObject, act.ObjectName)
			}
			if len(act.Columns) != tc.wantCols {
				t.Fatalf("expected %d columns, got %d: %v", tc.wantCols, len(act.Columns), act.Columns)
			}
			for _, f := range tc.wantFlags {
				assertAnalysisFlag(t, act.Flags, f)
			}
			if tc.wantIdx != "" && act.IndexType != tc.wantIdx {
				t.Fatalf("expected index type %q, got %q", tc.wantIdx, act.IndexType)
			}
			if tc.wantTable != "" {
				if len(res.Tables) != 1 || res.Tables[0].Name != tc.wantTable {
					t.Fatalf("expected table %q, got %+v", tc.wantTable, res.Tables)
				}
			}
		})
	}
}

// TestAnalyzeSQL_DDL_AlterTableDropColumn validates ALTER TABLE DROP COLUMN with IF EXISTS and CASCADE.
func TestAnalyzeSQL_DDL_AlterTableDropColumn(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantCol   string
		wantFlags []string
	}{
		{
			name:    "simple",
			sql:     "ALTER TABLE users DROP COLUMN email",
			wantCol: "email",
		},
		{
			name:      "IF EXISTS",
			sql:       "ALTER TABLE users DROP COLUMN IF EXISTS email",
			wantCol:   "email",
			wantFlags: []string{"IF_EXISTS"},
		},
		{
			name:      "CASCADE",
			sql:       "ALTER TABLE users DROP COLUMN email CASCADE",
			wantCol:   "email",
			wantFlags: []string{"CASCADE"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := AnalyzeSQL(tc.sql)
			if err != nil {
				t.Fatalf("AnalyzeSQL failed: %v", err)
			}
			if res.Command != SQLCommandDDL {
				t.Fatalf("expected DDL command, got %s", res.Command)
			}
			if len(res.DDLActions) != 1 {
				t.Fatalf("expected 1 DDL action, got %d: %+v", len(res.DDLActions), res.DDLActions)
			}
			act := res.DDLActions[0]
			if act.Type != "DROP_COLUMN" {
				t.Fatalf("expected DROP_COLUMN, got %s", act.Type)
			}
			if len(act.Columns) != 1 || act.Columns[0] != tc.wantCol {
				t.Fatalf("expected column %q, got %v", tc.wantCol, act.Columns)
			}
			for _, f := range tc.wantFlags {
				assertAnalysisFlag(t, act.Flags, f)
			}
			if len(res.Tables) != 1 || res.Tables[0].Name != "users" {
				t.Fatalf("expected table users, got %+v", res.Tables)
			}
		})
	}
}

// TestAnalyzeSQL_DDL_AlterTableAddColumn verifies ALTER TABLE ADD COLUMN metadata extraction.
func TestAnalyzeSQL_DDL_AlterTableAddColumn(t *testing.T) {
	res, err := AnalyzeSQL("ALTER TABLE users ADD COLUMN status text")
	if err != nil {
		t.Fatalf("AnalyzeSQL failed: %v", err)
	}
	if res.Command != SQLCommandDDL {
		t.Fatalf("expected DDL command, got %s", res.Command)
	}
	if len(res.DDLActions) != 1 {
		t.Fatalf("expected 1 DDL action, got %d", len(res.DDLActions))
	}
	act := res.DDLActions[0]
	if act.Type != "ALTER_TABLE" {
		t.Fatalf("expected ALTER_TABLE, got %s", act.Type)
	}
	assertAnalysisFlag(t, act.Flags, "ADD_COLUMN")
	if len(act.Columns) != 1 || act.Columns[0] != "status" {
		t.Fatalf("expected columns [status], got %v", act.Columns)
	}
}

// TestAnalyzeSQL_DDL_AlterTableMultiAction checks ALTER TABLE with combined ADD and DROP actions.
func TestAnalyzeSQL_DDL_AlterTableMultiAction(t *testing.T) {
	res, err := AnalyzeSQL("ALTER TABLE users ADD COLUMN status text, DROP COLUMN legacy")
	if err != nil {
		t.Fatalf("AnalyzeSQL failed: %v", err)
	}
	if res.Command != SQLCommandDDL {
		t.Fatalf("expected DDL command, got %s", res.Command)
	}
	if len(res.DDLActions) != 2 {
		t.Fatalf("expected 2 DDL actions, got %d: %+v", len(res.DDLActions), res.DDLActions)
	}
	// First: ADD COLUMN
	if res.DDLActions[0].Type != "ALTER_TABLE" {
		t.Fatalf("expected ALTER_TABLE for first action, got %s", res.DDLActions[0].Type)
	}
	assertAnalysisFlag(t, res.DDLActions[0].Flags, "ADD_COLUMN")
	if len(res.DDLActions[0].Columns) != 1 || res.DDLActions[0].Columns[0] != "status" {
		t.Fatalf("expected column [status], got %v", res.DDLActions[0].Columns)
	}
	// Second: DROP COLUMN
	if res.DDLActions[1].Type != "DROP_COLUMN" {
		t.Fatalf("expected DROP_COLUMN for second action, got %s", res.DDLActions[1].Type)
	}
	if len(res.DDLActions[1].Columns) != 1 || res.DDLActions[1].Columns[0] != "legacy" {
		t.Fatalf("expected column [legacy], got %v", res.DDLActions[1].Columns)
	}
}

// TestAnalyzeSQL_DDL_Truncate validates TRUNCATE with CASCADE, RESTRICT, and multi-table support.
func TestAnalyzeSQL_DDL_Truncate(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantCount  int
		wantFlags  []string
		wantTables int
	}{
		{
			name:       "simple",
			sql:        "TRUNCATE users",
			wantCount:  1,
			wantTables: 1,
		},
		{
			name:       "TABLE keyword",
			sql:        "TRUNCATE TABLE users",
			wantCount:  1,
			wantTables: 1,
		},
		{
			name:       "CASCADE",
			sql:        "TRUNCATE TABLE users CASCADE",
			wantCount:  1,
			wantFlags:  []string{"CASCADE"},
			wantTables: 1,
		},
		{
			name:       "RESTRICT",
			sql:        "TRUNCATE TABLE users RESTRICT",
			wantCount:  1,
			wantFlags:  []string{"RESTRICT"},
			wantTables: 1,
		},
		{
			name:       "multiple tables",
			sql:        "TRUNCATE users, orders",
			wantCount:  2,
			wantTables: 2,
		},
		{
			name:       "multiple tables CASCADE",
			sql:        "TRUNCATE TABLE users, orders CASCADE",
			wantCount:  2,
			wantFlags:  []string{"CASCADE"},
			wantTables: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := AnalyzeSQL(tc.sql)
			if err != nil {
				t.Fatalf("AnalyzeSQL failed: %v", err)
			}
			if res.Command != SQLCommandDDL {
				t.Fatalf("expected DDL command, got %s", res.Command)
			}
			if len(res.DDLActions) != tc.wantCount {
				t.Fatalf("expected %d DDL actions, got %d", tc.wantCount, len(res.DDLActions))
			}
			for _, act := range res.DDLActions {
				if act.Type != "TRUNCATE" {
					t.Fatalf("expected TRUNCATE, got %s", act.Type)
				}
				for _, f := range tc.wantFlags {
					assertAnalysisFlag(t, act.Flags, f)
				}
			}
			if len(res.Tables) != tc.wantTables {
				t.Fatalf("expected %d tables, got %d", tc.wantTables, len(res.Tables))
			}
		})
	}
}

func assertAnalysisFlag(t *testing.T, flags []string, flag string) {
	t.Helper()
	for _, f := range flags {
		if f == flag {
			return
		}
	}
	t.Fatalf("expected flag %q in %v", flag, flags)
}
