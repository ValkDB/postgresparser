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
		wantSchema string
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
			wantObject: "users",
			wantSchema: "myschema",
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
			if tc.wantSchema != "" && act.Schema != tc.wantSchema {
				t.Fatalf("expected schema %q, got %q", tc.wantSchema, act.Schema)
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
		name       string
		sql        string
		wantObject string
		wantSchema string
		wantFlags  []string
	}{
		{
			name:       "simple",
			sql:        "DROP INDEX idx_users_email",
			wantObject: "idx_users_email",
		},
		{
			name:       "IF EXISTS",
			sql:        "DROP INDEX IF EXISTS idx_users_email",
			wantObject: "idx_users_email",
			wantFlags:  []string{"IF_EXISTS"},
		},
		{
			name:       "CONCURRENTLY",
			sql:        "DROP INDEX CONCURRENTLY idx_users_email",
			wantObject: "idx_users_email",
			wantFlags:  []string{"CONCURRENTLY"},
		},
		{
			name:       "CONCURRENTLY IF EXISTS",
			sql:        "DROP INDEX CONCURRENTLY IF EXISTS idx_users_email",
			wantObject: "idx_users_email",
			wantFlags:  []string{"CONCURRENTLY", "IF_EXISTS"},
		},
		{
			name:       "schema-qualified",
			sql:        "DROP INDEX public.idx_users_email",
			wantObject: "idx_users_email",
			wantSchema: "public",
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
			if tc.wantObject != "" && act.ObjectName != tc.wantObject {
				t.Fatalf("expected object %q, got %q", tc.wantObject, act.ObjectName)
			}
			if tc.wantSchema != "" && act.Schema != tc.wantSchema {
				t.Fatalf("expected schema %q, got %q", tc.wantSchema, act.Schema)
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
		wantSchema string
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
		{
			name:       "schema-qualified table",
			sql:        "CREATE INDEX idx_email ON public.users (email)",
			wantObject: "idx_email",
			wantSchema: "public",
			wantCols:   1,
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
			if tc.wantSchema != "" && act.Schema != tc.wantSchema {
				t.Fatalf("expected schema %q, got %q", tc.wantSchema, act.Schema)
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

// TestAnalyzeSQL_DDL_CreateTable verifies CREATE TABLE metadata extraction.
func TestAnalyzeSQL_DDL_CreateTable(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    email text NOT NULL,
    name text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);`
	res, err := AnalyzeSQL(sql)
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
	if act.Type != "CREATE_TABLE" {
		t.Fatalf("expected CREATE_TABLE, got %s", act.Type)
	}
	if act.ObjectName != "users" {
		t.Fatalf("expected object users, got %q", act.ObjectName)
	}
	if act.Schema != "public" {
		t.Fatalf("expected schema public, got %q", act.Schema)
	}
	if len(act.Columns) != 4 {
		t.Fatalf("expected 4 columns, got %d: %v", len(act.Columns), act.Columns)
	}
	if len(act.ColumnDetails) != 4 {
		t.Fatalf("expected 4 column details, got %d: %+v", len(act.ColumnDetails), act.ColumnDetails)
	}

	want := []SQLDDLColumn{
		{Name: "id", Type: "integer", Nullable: false},
		{Name: "email", Type: "text", Nullable: false},
		{Name: "name", Type: "text", Nullable: true},
		{Name: "created_at", Type: "timestamp without time zone", Nullable: false, Default: "CURRENT_TIMESTAMP"},
	}
	for i := range want {
		if act.ColumnDetails[i] != want[i] {
			t.Fatalf("column detail %d mismatch: got %+v want %+v", i, act.ColumnDetails[i], want[i])
		}
	}

	if len(res.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d: %+v", len(res.Tables), res.Tables)
	}
	if res.Tables[0].Schema != "public" || res.Tables[0].Name != "users" {
		t.Fatalf("expected table public.users, got %+v", res.Tables[0])
	}
}

func TestAnalyzeSQL_DDL_CreateTableTypeCoverage(t *testing.T) {
	sql := `CREATE TABLE public.type_matrix (
    c_smallint smallint,
    c_integer integer,
    c_bigint bigint,
    c_numeric numeric(10,2),
    c_real real,
    c_double double precision,
    c_money money,
    c_bool boolean,
    c_char char(3),
    c_varchar varchar(50),
    c_text text,
    c_bytea bytea,
    c_date date,
    c_time time without time zone,
    c_timetz time with time zone,
    c_timestamp timestamp without time zone,
    c_timestamptz timestamp with time zone,
    c_interval interval year to month,
    c_uuid uuid,
    c_json json,
    c_jsonb jsonb,
    c_xml xml,
    c_inet inet,
    c_cidr cidr,
    c_macaddr macaddr,
    c_macaddr8 macaddr8,
    c_point point,
    c_line line,
    c_lseg lseg,
    c_box box,
    c_path path,
    c_polygon polygon,
    c_circle circle,
    c_int4range int4range,
    c_numrange numrange,
    c_tstzrange tstzrange,
    c_int_array integer[],
    c_text_array text[]
);`
	res, err := AnalyzeSQL(sql)
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
	if act.Type != "CREATE_TABLE" {
		t.Fatalf("expected CREATE_TABLE, got %s", act.Type)
	}
	if act.Schema != "public" {
		t.Fatalf("expected schema public, got %q", act.Schema)
	}
	if act.ObjectName != "type_matrix" {
		t.Fatalf("expected object type_matrix, got %q", act.ObjectName)
	}

	wantTypes := map[string]string{
		"c_smallint":    "smallint",
		"c_integer":     "integer",
		"c_bigint":      "bigint",
		"c_numeric":     "numeric(10,2)",
		"c_real":        "real",
		"c_double":      "double precision",
		"c_money":       "money",
		"c_bool":        "boolean",
		"c_char":        "char(3)",
		"c_varchar":     "varchar(50)",
		"c_text":        "text",
		"c_bytea":       "bytea",
		"c_date":        "date",
		"c_time":        "time without time zone",
		"c_timetz":      "time with time zone",
		"c_timestamp":   "timestamp without time zone",
		"c_timestamptz": "timestamp with time zone",
		"c_interval":    "interval year to month",
		"c_uuid":        "uuid",
		"c_json":        "json",
		"c_jsonb":       "jsonb",
		"c_xml":         "xml",
		"c_inet":        "inet",
		"c_cidr":        "cidr",
		"c_macaddr":     "macaddr",
		"c_macaddr8":    "macaddr8",
		"c_point":       "point",
		"c_line":        "line",
		"c_lseg":        "lseg",
		"c_box":         "box",
		"c_path":        "path",
		"c_polygon":     "polygon",
		"c_circle":      "circle",
		"c_int4range":   "int4range",
		"c_numrange":    "numrange",
		"c_tstzrange":   "tstzrange",
		"c_int_array":   "integer[]",
		"c_text_array":  "text[]",
	}

	if len(act.ColumnDetails) != len(wantTypes) {
		t.Fatalf("expected %d column details, got %d", len(wantTypes), len(act.ColumnDetails))
	}
	got := make(map[string]SQLDDLColumn, len(act.ColumnDetails))
	for _, col := range act.ColumnDetails {
		got[col.Name] = col
	}
	for colName, wantType := range wantTypes {
		col, ok := got[colName]
		if !ok {
			t.Fatalf("missing column %q", colName)
		}
		if col.Type != wantType {
			t.Fatalf("type mismatch for %s: got %q want %q", colName, col.Type, wantType)
		}
		if !col.Nullable {
			t.Fatalf("expected nullable=true by default for %s", colName)
		}
		if col.Default != "" {
			t.Fatalf("expected empty default for %s, got %q", colName, col.Default)
		}
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

func TestAnalyzeSQL_DDL_AlterTableSchemaQualified(t *testing.T) {
	res, err := AnalyzeSQL("ALTER TABLE public.users ADD COLUMN status text")
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
	if act.Schema != "public" {
		t.Fatalf("expected schema public, got %q", act.Schema)
	}
	if act.ObjectName != "users" {
		t.Fatalf("expected object users, got %q", act.ObjectName)
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
		wantObject string
		wantSchema string
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
		{
			name:       "schema-qualified",
			sql:        "TRUNCATE public.users",
			wantCount:  1,
			wantObject: "users",
			wantSchema: "public",
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
			if tc.wantObject != "" {
				if res.DDLActions[0].ObjectName != tc.wantObject {
					t.Fatalf("expected object %q, got %q", tc.wantObject, res.DDLActions[0].ObjectName)
				}
			}
			if tc.wantSchema != "" {
				if res.DDLActions[0].Schema != tc.wantSchema {
					t.Fatalf("expected schema %q, got %q", tc.wantSchema, res.DDLActions[0].Schema)
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
