// parser_ir_ddl_test.go exercises DDL statement parsing at the IR level.
package postgresparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIR_DDL_DropTable(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantActions int
		wantType    DDLActionType
		wantObject  string
		wantSchema  string
		wantFlags   []string
		wantTables  int
	}{
		{
			name:        "simple",
			sql:         "DROP TABLE users",
			wantActions: 1,
			wantType:    DDLDropTable,
			wantObject:  "users",
			wantTables:  1,
		},
		{
			name:        "IF EXISTS",
			sql:         "DROP TABLE IF EXISTS users",
			wantActions: 1,
			wantType:    DDLDropTable,
			wantObject:  "users",
			wantFlags:   []string{"IF_EXISTS"},
			wantTables:  1,
		},
		{
			name:        "CASCADE",
			sql:         "DROP TABLE users CASCADE",
			wantActions: 1,
			wantType:    DDLDropTable,
			wantObject:  "users",
			wantFlags:   []string{"CASCADE"},
			wantTables:  1,
		},
		{
			name:        "schema-qualified",
			sql:         "DROP TABLE myschema.users",
			wantActions: 1,
			wantType:    DDLDropTable,
			wantObject:  "users",
			wantSchema:  "myschema",
			wantTables:  1,
		},
		{
			name:        "multiple tables",
			sql:         "DROP TABLE users, orders, products",
			wantActions: 3,
			wantType:    DDLDropTable,
			wantTables:  3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ir := parseAssertNoError(t, tc.sql)
			assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
			require.Len(t, ir.DDLActions, tc.wantActions, "action count mismatch")

			act := ir.DDLActions[0]
			assert.Equal(t, tc.wantType, act.Type, "action type mismatch")
			if tc.wantObject != "" {
				assert.Equal(t, tc.wantObject, act.ObjectName, "object name mismatch")
			}
			if tc.wantSchema != "" {
				assert.Equal(t, tc.wantSchema, act.Schema, "schema mismatch")
			}
			assert.Subset(t, act.Flags, tc.wantFlags, "flags mismatch")
			assert.Len(t, ir.Tables, tc.wantTables, "tables count mismatch")
		})
	}
}

func TestIR_DDL_DropIndex(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantActions int
		wantObject  string
		wantSchema  string
		wantFlags   []string
	}{
		{
			name:        "simple",
			sql:         "DROP INDEX idx_users_email",
			wantActions: 1,
			wantObject:  "idx_users_email",
		},
		{
			name:        "CONCURRENTLY",
			sql:         "DROP INDEX CONCURRENTLY idx_users_email",
			wantActions: 1,
			wantObject:  "idx_users_email",
			wantFlags:   []string{"CONCURRENTLY"},
		},
		{
			name:        "IF EXISTS",
			sql:         "DROP INDEX IF EXISTS idx_users_email",
			wantActions: 1,
			wantObject:  "idx_users_email",
			wantFlags:   []string{"IF_EXISTS"},
		},
		{
			name:        "schema-qualified",
			sql:         "DROP INDEX public.idx_users_email",
			wantActions: 1,
			wantObject:  "idx_users_email",
			wantSchema:  "public",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ir := parseAssertNoError(t, tc.sql)
			assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
			require.Len(t, ir.DDLActions, tc.wantActions, "action count mismatch")

			act := ir.DDLActions[0]
			assert.Equal(t, DDLDropIndex, act.Type, "expected DROP_INDEX")
			assert.Equal(t, tc.wantObject, act.ObjectName, "object name mismatch")
			if tc.wantSchema != "" {
				assert.Equal(t, tc.wantSchema, act.Schema, "schema mismatch")
			}
			assert.Subset(t, act.Flags, tc.wantFlags, "flags mismatch")
		})
	}
}

func TestIR_DDL_CreateIndex(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantObject string
		wantSchema string
		wantCols   int
		wantFlags  []string
		wantIdx    string
		wantTables int
	}{
		{
			name:       "simple",
			sql:        "CREATE INDEX idx_email ON users (email)",
			wantObject: "idx_email",
			wantCols:   1,
			wantTables: 1,
		},
		{
			name:       "CONCURRENTLY",
			sql:        "CREATE INDEX CONCURRENTLY idx_email ON users (email)",
			wantObject: "idx_email",
			wantCols:   1,
			wantFlags:  []string{"CONCURRENTLY"},
			wantTables: 1,
		},
		{
			name:       "UNIQUE",
			sql:        "CREATE UNIQUE INDEX idx_email ON users (email)",
			wantObject: "idx_email",
			wantCols:   1,
			wantFlags:  []string{"UNIQUE"},
			wantTables: 1,
		},
		{
			name:       "USING gin",
			sql:        "CREATE INDEX idx_tags ON posts USING gin (tags)",
			wantObject: "idx_tags",
			wantCols:   1,
			wantIdx:    "gin",
			wantTables: 1,
		},
		{
			name:       "USING btree",
			sql:        "CREATE INDEX idx_name ON users USING btree (name)",
			wantObject: "idx_name",
			wantCols:   1,
			wantIdx:    "btree",
			wantTables: 1,
		},
		{
			name:       "multi-column",
			sql:        "CREATE INDEX idx_compound ON users (last_name, first_name)",
			wantObject: "idx_compound",
			wantCols:   2,
			wantTables: 1,
		},
		{
			name:       "IF NOT EXISTS",
			sql:        "CREATE INDEX IF NOT EXISTS idx_email ON users (email)",
			wantObject: "idx_email",
			wantCols:   1,
			wantFlags:  []string{"IF_NOT_EXISTS"},
			wantTables: 1,
		},
		{
			name:       "schema-qualified table",
			sql:        "CREATE INDEX idx_email ON public.users (email)",
			wantObject: "idx_email",
			wantSchema: "public",
			wantCols:   1,
			wantTables: 1,
		},
		{
			name:       "schema-qualified index and table",
			sql:        "CREATE UNIQUE INDEX public.idx_users_email ON public.users (email)",
			wantObject: "idx_users_email",
			wantSchema: "public",
			wantCols:   1,
			wantFlags:  []string{"UNIQUE"},
			wantTables: 1,
		},
		{
			name:       "IF NOT EXISTS with schema-qualified index and table",
			sql:        "CREATE INDEX IF NOT EXISTS public.idx_users_email ON public.users (email)",
			wantObject: "idx_users_email",
			wantSchema: "public",
			wantCols:   1,
			wantFlags:  []string{"IF_NOT_EXISTS"},
			wantTables: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ir := parseAssertNoError(t, tc.sql)
			assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
			require.Len(t, ir.DDLActions, 1, "action count mismatch")

			act := ir.DDLActions[0]
			assert.Equal(t, DDLCreateIndex, act.Type, "expected CREATE_INDEX")
			assert.Equal(t, tc.wantObject, act.ObjectName, "object name mismatch")
			if tc.wantSchema != "" {
				assert.Equal(t, tc.wantSchema, act.Schema, "schema mismatch")
			}
			assert.Len(t, act.Columns, tc.wantCols, "column count mismatch")
			assert.Subset(t, act.Flags, tc.wantFlags, "flags mismatch")

			if tc.wantIdx != "" {
				assert.Equal(t, tc.wantIdx, act.IndexType, "index type mismatch")
			}
			assert.Len(t, ir.Tables, tc.wantTables, "tables count mismatch")
		})
	}
}

func TestIR_DDL_CreateTable(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    email text NOT NULL,
    name text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.DDLActions, 1, "action count mismatch")

	act := ir.DDLActions[0]
	assert.Equal(t, DDLCreateTable, act.Type, "expected CREATE_TABLE")
	assert.Equal(t, "users", act.ObjectName, "object name mismatch")
	assert.Equal(t, "public", act.Schema, "schema mismatch")
	assert.Equal(t, []string{"id", "email", "name", "created_at"}, act.Columns, "column names mismatch")
	assert.Equal(t, []DDLColumn{
		{Name: "id", Type: "integer", Nullable: false},
		{Name: "email", Type: "text", Nullable: false},
		{Name: "name", Type: "text", Nullable: true},
		{Name: "created_at", Type: "timestamp without time zone", Nullable: false, Default: "CURRENT_TIMESTAMP"},
	}, act.ColumnDetails, "column details mismatch")

	require.Len(t, ir.Tables, 1, "tables count mismatch")
	assert.Equal(t, "public", ir.Tables[0].Schema, "table schema mismatch")
	assert.Equal(t, "users", ir.Tables[0].Name, "table name mismatch")
}

func TestIR_DDL_CreateTableIfNotExists(t *testing.T) {
	ir := parseAssertNoError(t, "CREATE TABLE IF NOT EXISTS users (id integer)")
	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.DDLActions, 1, "action count mismatch")

	act := ir.DDLActions[0]
	assert.Equal(t, DDLCreateTable, act.Type, "expected CREATE_TABLE")
	assert.Equal(t, "users", act.ObjectName, "object name mismatch")
	assert.Subset(t, act.Flags, []string{"IF_NOT_EXISTS"}, "flags mismatch")
	require.Len(t, act.ColumnDetails, 1, "column details mismatch")
	assert.Equal(t, DDLColumn{Name: "id", Type: "integer", Nullable: true}, act.ColumnDetails[0], "column mismatch")
}

func TestIR_DDL_CreateTable_TablePrimaryKeySetsNullableFalse(t *testing.T) {
	sql := `CREATE TABLE public.accounts (
    id integer,
    tenant_id integer,
    payload text,
    CONSTRAINT accounts_pk PRIMARY KEY (id, tenant_id)
);`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.DDLActions, 1, "action count mismatch")

	act := ir.DDLActions[0]
	assert.Equal(t, DDLCreateTable, act.Type, "expected CREATE_TABLE")
	assert.Equal(t, "accounts", act.ObjectName, "object name mismatch")
	assert.Equal(t, "public", act.Schema, "schema mismatch")
	assert.Equal(t, []string{"id", "tenant_id", "payload"}, act.Columns, "column names mismatch")
	assert.Equal(t, []DDLColumn{
		{Name: "id", Type: "integer", Nullable: false},
		{Name: "tenant_id", Type: "integer", Nullable: false},
		{Name: "payload", Type: "text", Nullable: true},
	}, act.ColumnDetails, "column details mismatch")

	require.Len(t, ir.Tables, 1, "tables count mismatch")
	assert.Equal(t, "public", ir.Tables[0].Schema, "table schema mismatch")
	assert.Equal(t, "accounts", ir.Tables[0].Name, "table name mismatch")
}

func TestIR_DDL_CreateTable_TablePrimaryKeySetsNullableFalse_NoSchema(t *testing.T) {
	sql := `CREATE TABLE accounts (
    id integer,
    tenant_id integer,
    payload text,
    PRIMARY KEY (id, tenant_id)
);`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.DDLActions, 1, "action count mismatch")

	act := ir.DDLActions[0]
	assert.Equal(t, DDLCreateTable, act.Type, "expected CREATE_TABLE")
	assert.Equal(t, "accounts", act.ObjectName, "object name mismatch")
	assert.Empty(t, act.Schema, "schema mismatch")
	assert.Equal(t, []string{"id", "tenant_id", "payload"}, act.Columns, "column names mismatch")
	assert.Equal(t, []DDLColumn{
		{Name: "id", Type: "integer", Nullable: false},
		{Name: "tenant_id", Type: "integer", Nullable: false},
		{Name: "payload", Type: "text", Nullable: true},
	}, act.ColumnDetails, "column details mismatch")

	require.Len(t, ir.Tables, 1, "tables count mismatch")
	assert.Empty(t, ir.Tables[0].Schema, "table schema mismatch")
	assert.Equal(t, "accounts", ir.Tables[0].Name, "table name mismatch")
}

func TestIR_DDL_CreateTableTypeCoverage(t *testing.T) {
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
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.DDLActions, 1, "action count mismatch")
	act := ir.DDLActions[0]
	assert.Equal(t, DDLCreateTable, act.Type, "expected CREATE_TABLE")
	assert.Equal(t, "public", act.Schema, "schema mismatch")
	assert.Equal(t, "type_matrix", act.ObjectName, "object name mismatch")

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

	require.Len(t, act.ColumnDetails, len(wantTypes), "column count mismatch")
	got := make(map[string]DDLColumn, len(act.ColumnDetails))
	for _, col := range act.ColumnDetails {
		got[col.Name] = col
	}
	for colName, wantType := range wantTypes {
		col, ok := got[colName]
		require.Truef(t, ok, "missing column %q", colName)
		assert.Equal(t, wantType, col.Type, "type mismatch for %s", colName)
		assert.True(t, col.Nullable, "expected nullable=true by default for %s", colName)
		assert.Empty(t, col.Default, "expected no default for %s", colName)
	}
}

func TestIR_DDL_AlterTableDropColumn(t *testing.T) {
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
			ir := parseAssertNoError(t, tc.sql)
			assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
			require.Len(t, ir.DDLActions, 1, "action count mismatch")

			act := ir.DDLActions[0]
			assert.Equal(t, DDLDropColumn, act.Type, "expected DROP_COLUMN")
			require.Len(t, act.Columns, 1, "column count mismatch")
			assert.Equal(t, tc.wantCol, act.Columns[0], "column name mismatch")
			assert.Subset(t, act.Flags, tc.wantFlags, "flags mismatch")
			assert.True(t, containsTable(ir.Tables, "users"), "expected table 'users'")
		})
	}
}

func TestIR_DDL_AlterTableAddColumn(t *testing.T) {
	ir := parseAssertNoError(t, "ALTER TABLE users ADD COLUMN status text")
	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.DDLActions, 1, "action count mismatch")

	act := ir.DDLActions[0]
	assert.Equal(t, DDLAlterTable, act.Type, "expected ALTER_TABLE")
	require.Len(t, act.Columns, 1, "column count mismatch")
	assert.Equal(t, "status", act.Columns[0], "column mismatch")
	assert.Contains(t, act.Flags, "ADD_COLUMN", "expected flag ADD_COLUMN")
}

func TestIR_DDL_AlterTableSchemaQualified(t *testing.T) {
	ir := parseAssertNoError(t, "ALTER TABLE public.users ADD COLUMN status text")
	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.DDLActions, 1, "action count mismatch")

	act := ir.DDLActions[0]
	assert.Equal(t, DDLAlterTable, act.Type, "expected ALTER_TABLE")
	assert.Equal(t, "public", act.Schema, "schema mismatch")
	assert.Equal(t, "users", act.ObjectName, "object name mismatch")
}

func TestIR_DDL_AlterTableMultiAction(t *testing.T) {
	ir := parseAssertNoError(t, "ALTER TABLE users ADD COLUMN status text, DROP COLUMN legacy")
	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.DDLActions, 2, "action count mismatch")

	// First action: ADD COLUMN
	assert.Equal(t, DDLAlterTable, ir.DDLActions[0].Type, "expected ALTER_TABLE for first action")
	assert.Contains(t, ir.DDLActions[0].Flags, "ADD_COLUMN", "expected flag ADD_COLUMN")

	// Second action: DROP COLUMN
	assert.Equal(t, DDLDropColumn, ir.DDLActions[1].Type, "expected DROP_COLUMN for second action")
	require.Len(t, ir.DDLActions[1].Columns, 1, "column count mismatch")
	assert.Equal(t, "legacy", ir.DDLActions[1].Columns[0], "column name mismatch")
}

func TestIR_DDL_Truncate(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantActions int
		wantFlags   []string
		wantTables  int
	}{
		{
			name:        "simple",
			sql:         "TRUNCATE users",
			wantActions: 1,
			wantTables:  1,
		},
		{
			name:        "TABLE keyword",
			sql:         "TRUNCATE TABLE users",
			wantActions: 1,
			wantTables:  1,
		},
		{
			name:        "CASCADE",
			sql:         "TRUNCATE TABLE users CASCADE",
			wantActions: 1,
			wantFlags:   []string{"CASCADE"},
			wantTables:  1,
		},
		{
			name:        "multiple tables",
			sql:         "TRUNCATE users, orders",
			wantActions: 2,
			wantTables:  2,
		},
		{
			name:        "schema-qualified",
			sql:         "TRUNCATE public.users",
			wantActions: 1,
			wantTables:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ir := parseAssertNoError(t, tc.sql)
			assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
			require.Len(t, ir.DDLActions, tc.wantActions, "action count mismatch")

			for _, act := range ir.DDLActions {
				assert.Equal(t, DDLTruncate, act.Type, "expected TRUNCATE type")
			}
			assert.Subset(t, ir.DDLActions[0].Flags, tc.wantFlags, "flags mismatch")
			if tc.name == "schema-qualified" {
				assert.Equal(t, "public", ir.DDLActions[0].Schema, "schema mismatch")
				assert.Equal(t, "users", ir.DDLActions[0].ObjectName, "object name mismatch")
			}
			assert.Len(t, ir.Tables, tc.wantTables, "tables count mismatch")
		})
	}
}
