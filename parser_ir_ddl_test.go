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
			name:       "schema-qualified index on unqualified table",
			sql:        "CREATE INDEX analytics.idx_users_email ON users (email)",
			wantObject: "idx_users_email",
			wantSchema: "analytics",
			wantCols:   1,
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
		{
			name:       "quoted schema-qualified index and table",
			sql:        `CREATE UNIQUE INDEX "analytics"."IdxUsersEmail" ON "public"."users" ("email")`,
			wantObject: `"IdxUsersEmail"`,
			wantSchema: `"analytics"`,
			wantCols:   1,
			wantFlags:  []string{"UNIQUE"},
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

func TestIR_DDL_CreateIndex_QualifiedIndexNameNormalization(t *testing.T) {
	ir := parseAssertNoError(t, "CREATE INDEX analytics.idx_users_email ON public.users (email)")
	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.DDLActions, 1, "action count mismatch")

	act := ir.DDLActions[0]
	assert.Equal(t, DDLCreateIndex, act.Type, "expected CREATE_INDEX")
	assert.Equal(t, "idx_users_email", act.ObjectName, "object name mismatch")
	assert.Equal(t, "analytics", act.Schema, "index schema should come from index name")

	require.Len(t, ir.Tables, 1, "tables count mismatch")
	assert.Equal(t, "public", ir.Tables[0].Schema, "table schema mismatch")
	assert.Equal(t, "users", ir.Tables[0].Name, "table name mismatch")
}

func TestIR_DDL_CreateIndex_OnlyRelationRawPreserved(t *testing.T) {
	ir := parseAssertNoError(t, "CREATE INDEX idx_users_email ON ONLY public.users (email)")
	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.DDLActions, 1, "action count mismatch")

	act := ir.DDLActions[0]
	assert.Equal(t, DDLCreateIndex, act.Type, "expected CREATE_INDEX")
	assert.Equal(t, "idx_users_email", act.ObjectName, "object name mismatch")
	assert.Equal(t, "public", act.Schema, "index schema should inherit parsed table schema")

	require.Len(t, ir.Tables, 1, "tables count mismatch")
	assert.Equal(t, "public", ir.Tables[0].Schema, "table schema mismatch")
	assert.Equal(t, "users", ir.Tables[0].Name, "table name mismatch")
	assert.Equal(t, "ONLY public.users", ir.Tables[0].Raw, "table raw mismatch")
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

func TestIR_DDL_CommentOn_Table(t *testing.T) {
	tests := []struct {
		name           string
		sql            string
		wantObjectType string
		wantSchema     string
		wantObjectName string
		wantTarget     string
		wantColumns    []string
		wantComment    string
		wantTables     int
		wantFirstTable *TableRef
	}{
		{
			name:           "issue34 table comment",
			sql:            `COMMENT ON TABLE public.users IS 'Stores user account information';`,
			wantObjectType: "TABLE",
			wantSchema:     "public",
			wantObjectName: "users",
			wantTarget:     "public.users",
			wantComment:    "Stores user account information",
			wantTables:     1,
			wantFirstTable: &TableRef{Schema: "public", Name: "users", Type: TableTypeBase, Raw: "public.users"},
		},
		{
			name:           "issue34 column comment",
			sql:            `COMMENT ON COLUMN public.users.email IS 'User email address, must be unique';`,
			wantObjectType: "COLUMN",
			wantSchema:     "public",
			wantObjectName: "users",
			wantTarget:     "public.users.email",
			wantColumns:    []string{"email"},
			wantComment:    "User email address, must be unique",
			wantTables:     1,
			wantFirstTable: &TableRef{Schema: "public", Name: "users", Type: TableTypeBase, Raw: "public.users"},
		},
		{
			name:           "issue34 index comment",
			sql:            `COMMENT ON INDEX public.idx_bookings_dates IS 'Composite index for efficient date range queries on bookings';`,
			wantObjectType: "INDEX",
			wantSchema:     "public",
			wantObjectName: "idx_bookings_dates",
			wantTarget:     "public.idx_bookings_dates",
			wantComment:    "Composite index for efficient date range queries on bookings",
			wantTables:     0,
		},
		{
			name:           "unqualified column target",
			sql:            `COMMENT ON COLUMN users.email IS 'x';`,
			wantObjectType: "COLUMN",
			wantObjectName: "users",
			wantTarget:     "users.email",
			wantColumns:    []string{"email"},
			wantComment:    "x",
			wantTables:     1,
			wantFirstTable: &TableRef{Name: "users", Type: TableTypeBase, Raw: "users"},
		},
		{
			name:           "quoted dotted identifiers in column target",
			sql:            `COMMENT ON COLUMN public."my.table"."my.col" IS 'x';`,
			wantObjectType: "COLUMN",
			wantSchema:     "public",
			wantObjectName: `"my.table"`,
			wantTarget:     `public."my.table"."my.col"`,
			wantColumns:    []string{`"my.col"`},
			wantComment:    "x",
			wantTables:     1,
			wantFirstTable: &TableRef{Schema: "public", Name: `"my.table"`, Type: TableTypeBase, Raw: `public."my.table"`},
		},
		{
			name:           "unquoted dotted identifiers are treated as qualifiers",
			sql:            `COMMENT ON COLUMN public.my.table.my.col IS 'x';`,
			wantObjectType: "COLUMN",
			wantSchema:     "public.my.table",
			wantObjectName: "my",
			wantTarget:     "public.my.table.my.col",
			wantColumns:    []string{"col"},
			wantComment:    "x",
			wantTables:     1,
			wantFirstTable: &TableRef{Schema: "public.my.table", Name: "my", Type: TableTypeBase, Raw: "public.my.table.my"},
		},
		{
			name:           "null comment literal",
			sql:            `COMMENT ON TABLE public.users IS NULL;`,
			wantObjectType: "TABLE",
			wantSchema:     "public",
			wantObjectName: "users",
			wantTarget:     "public.users",
			wantComment:    "",
			wantTables:     1,
			wantFirstTable: &TableRef{Schema: "public", Name: "users", Type: TableTypeBase, Raw: "public.users"},
		},
		{
			name:           "escaped string literal",
			sql:            `COMMENT ON TABLE public.users IS E'line1\nline2';`,
			wantObjectType: "TABLE",
			wantSchema:     "public",
			wantObjectName: "users",
			wantTarget:     "public.users",
			wantComment:    "line1\nline2",
			wantTables:     1,
			wantFirstTable: &TableRef{Schema: "public", Name: "users", Type: TableTypeBase, Raw: "public.users"},
		},
		{
			name:           "dollar quoted string literal",
			sql:            `COMMENT ON TABLE public.users IS $tag$Stores "quoted" data$tag$;`,
			wantObjectType: "TABLE",
			wantSchema:     "public",
			wantObjectName: "users",
			wantTarget:     "public.users",
			wantComment:    `Stores "quoted" data`,
			wantTables:     1,
			wantFirstTable: &TableRef{Schema: "public", Name: "users", Type: TableTypeBase, Raw: "public.users"},
		},
		{
			name:           "foreign table comment",
			sql:            `COMMENT ON FOREIGN TABLE public.remote_users IS 'Foreign mirror';`,
			wantObjectType: "FOREIGN TABLE",
			wantSchema:     "public",
			wantObjectName: "remote_users",
			wantTarget:     "public.remote_users",
			wantComment:    "Foreign mirror",
			wantTables:     1,
			wantFirstTable: &TableRef{Schema: "public", Name: "remote_users", Type: TableTypeBase, Raw: "public.remote_users"},
		},
		{
			name:           "type comment",
			sql:            `COMMENT ON TYPE public.email_address IS 'Type used for email addresses';`,
			wantObjectType: "TYPE",
			wantSchema:     "public",
			wantObjectName: "email_address",
			wantTarget:     "public.email_address",
			wantComment:    "Type used for email addresses",
			wantTables:     0,
		},
		{
			name:           "schema comment",
			sql:            `COMMENT ON SCHEMA public IS 'Application schema';`,
			wantObjectType: "SCHEMA",
			wantObjectName: "public",
			wantTarget:     "public",
			wantComment:    "Application schema",
			wantTables:     0,
		},
		{
			name:           "unknown object type (FUNCTION)",
			sql:            `COMMENT ON FUNCTION public.my_func(integer) IS 'Does something';`,
			wantObjectType: "UNKNOWN",
			wantComment:    "Does something",
			wantTables:     0,
		},
		{
			name:           "doubled single-quote escaping",
			sql:            `COMMENT ON TABLE users IS 'it''s a test';`,
			wantObjectType: "TABLE",
			wantObjectName: "users",
			wantTarget:     "users",
			wantComment:    "it's a test",
			wantTables:     1,
			wantFirstTable: &TableRef{Name: "users", Type: TableTypeBase, Raw: "users"},
		},
		{
			name:           "empty string comment",
			sql:            `COMMENT ON TABLE users IS '';`,
			wantObjectType: "TABLE",
			wantObjectName: "users",
			wantTarget:     "users",
			wantComment:    "",
			wantTables:     1,
			wantFirstTable: &TableRef{Name: "users", Type: TableTypeBase, Raw: "users"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ir := parseAssertNoError(t, tc.sql)

			assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
			require.Len(t, ir.DDLActions, 1, "action count mismatch")
			act := ir.DDLActions[0]
			assert.Equal(t, DDLComment, act.Type, "expected COMMENT action")
			assert.Equal(t, tc.wantObjectType, act.ObjectType, "object type mismatch")
			assert.Equal(t, tc.wantSchema, act.Schema, "schema mismatch")
			assert.Equal(t, tc.wantObjectName, act.ObjectName, "object name mismatch")
			assert.Equal(t, tc.wantTarget, act.Target, "target mismatch")
			assert.Equal(t, tc.wantColumns, act.Columns, "column list mismatch")
			assert.Equal(t, tc.wantComment, act.Comment, "comment mismatch")

			require.Len(t, ir.Tables, tc.wantTables, "tables count mismatch")
			if tc.wantFirstTable != nil {
				assert.Equal(t, *tc.wantFirstTable, ir.Tables[0], "first table ref mismatch")
			}
		})
	}
}

func TestIR_DDL_CreateTableFieldComments_Table(t *testing.T) {
	tests := []struct {
		name               string
		sql                string
		opts               ParseOptions
		wantCommentsByCol  map[string][]string
		wantColumnSequence []string
	}{
		{
			name: "issue25 example",
			sql: `CREATE TABLE public.users (
    -- [Attribute("Just an example")]
    -- required, min 5, max 55
    name        text,

    -- single-column FK, inline
    org_id      integer     REFERENCES public.organizations(id)
);`,
			opts: ParseOptions{IncludeCreateTableFieldComments: true},
			wantCommentsByCol: map[string][]string{
				"name":   {`[Attribute("Just an example")]`, "required, min 5, max 55"},
				"org_id": {"single-column FK, inline"},
			},
			wantColumnSequence: []string{"name", "org_id"},
		},
		{
			name: "disabled by default",
			sql: `CREATE TABLE public.users (
    -- should not be extracted by default
    name text
);`,
			wantCommentsByCol: map[string][]string{
				"name": {},
			},
			wantColumnSequence: []string{"name"},
		},
		{
			name: "skips constraint comments",
			sql: `CREATE TABLE public.users (
    -- user id
    id integer,
    -- should not attach to any column
    CONSTRAINT users_pk PRIMARY KEY (id),
    -- user email
    email text
);`,
			opts: ParseOptions{IncludeCreateTableFieldComments: true},
			wantCommentsByCol: map[string][]string{
				"id":    {"user id"},
				"email": {"user email"},
			},
			wantColumnSequence: []string{"id", "email"},
		},
		{
			name: "quoted and unquoted identifiers",
			sql: `CREATE TABLE public.users (
    -- case-sensitive display email
    "Email" text,
    -- lowercase fallback
    email text
);`,
			opts: ParseOptions{IncludeCreateTableFieldComments: true},
			wantCommentsByCol: map[string][]string{
				`"Email"`: {"case-sensitive display email"},
				"email":   {"lowercase fallback"},
			},
			wantColumnSequence: []string{`"Email"`, "email"},
		},
		{
			name: "handles defaults with commas and functions",
			sql: `CREATE TABLE public.events (
    -- payload metadata
    payload jsonb DEFAULT jsonb_build_object('a', 1, 'b', 2),
    -- created marker
    created_at timestamptz DEFAULT now()
);`,
			opts: ParseOptions{IncludeCreateTableFieldComments: true},
			wantCommentsByCol: map[string][]string{
				"payload":    {"payload metadata"},
				"created_at": {"created marker"},
			},
			wantColumnSequence: []string{"payload", "created_at"},
		},
		{
			name: "inline trailing comments are treated as next-element comments",
			sql: `CREATE TABLE public.users (
    id integer, -- not attached
    -- attached to email
    email text
);`,
			opts: ParseOptions{IncludeCreateTableFieldComments: true},
			wantCommentsByCol: map[string][]string{
				"id":    {},
				"email": {"not attached", "attached to email"},
			},
			wantColumnSequence: []string{"id", "email"},
		},
		{
			name: "line comments retained while block comments ignored",
			sql: `CREATE TABLE public.users (
    /* ignored */
    -- picked up
    name text
);`,
			opts: ParseOptions{IncludeCreateTableFieldComments: true},
			wantCommentsByCol: map[string][]string{
				"name": {"picked up"},
			},
			wantColumnSequence: []string{"name"},
		},
	}

	commentsByName := func(cols []DDLColumn) map[string][]string {
		out := make(map[string][]string, len(cols))
		for _, col := range cols {
			buf := make([]string, len(col.Comment))
			copy(buf, col.Comment)
			out[col.Name] = buf
		}
		return out
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				ir  *ParsedQuery
				err error
			)
			if tc.opts == (ParseOptions{}) {
				ir = parseAssertNoError(t, tc.sql)
			} else {
				ir, err = ParseSQLWithOptions(tc.sql, tc.opts)
				require.NoError(t, err, "ParseSQLWithOptions failed")
			}

			assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
			require.Len(t, ir.DDLActions, 1, "action count mismatch")
			act := ir.DDLActions[0]
			require.Len(t, act.ColumnDetails, len(tc.wantColumnSequence), "column count mismatch")

			gotSequence := make([]string, 0, len(act.ColumnDetails))
			for _, col := range act.ColumnDetails {
				gotSequence = append(gotSequence, col.Name)
			}
			assert.Equal(t, tc.wantColumnSequence, gotSequence, "column order mismatch")
			assert.Equal(t, tc.wantCommentsByCol, commentsByName(act.ColumnDetails), "column comments mismatch")
		})
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

func TestIR_DDL_AlterTableOnlySchemaQualifiedTableRef(t *testing.T) {
	sql := `ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.Tables, 1, "tables count mismatch")
	assert.Equal(t, "public", ir.Tables[0].Schema, "table schema mismatch")
	assert.Equal(t, "schema_migrations", ir.Tables[0].Name, "table name mismatch")
	assert.Equal(t, "ONLY public.schema_migrations", ir.Tables[0].Raw, "table raw mismatch")

	// ADD CONSTRAINT is currently skipped in DDL action extraction.
	assert.Empty(t, ir.DDLActions, "expected no DDL actions for ADD CONSTRAINT")
}

func TestIR_DDL_AlterTableOnlyUnqualifiedTableRef(t *testing.T) {
	sql := `ALTER TABLE ONLY schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.Tables, 1, "tables count mismatch")
	assert.Equal(t, "", ir.Tables[0].Schema, "table schema mismatch")
	assert.Equal(t, "schema_migrations", ir.Tables[0].Name, "table name mismatch")
	assert.Equal(t, "ONLY schema_migrations", ir.Tables[0].Raw, "table raw mismatch")

	// ADD CONSTRAINT is currently skipped in DDL action extraction.
	assert.Empty(t, ir.DDLActions, "expected no DDL actions for ADD CONSTRAINT")
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
		wantSchema  string
		wantObject  string
		wantRaw     string
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
			wantSchema:  "public",
			wantObject:  "users",
			wantRaw:     "public.users",
		},
		{
			name:        "only schema-qualified",
			sql:         "TRUNCATE ONLY public.users",
			wantActions: 1,
			wantTables:  1,
			wantSchema:  "public",
			wantObject:  "users",
			wantRaw:     "ONLY public.users",
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
			if tc.wantSchema != "" {
				assert.Equal(t, tc.wantSchema, ir.DDLActions[0].Schema, "schema mismatch")
			}
			if tc.wantObject != "" {
				assert.Equal(t, tc.wantObject, ir.DDLActions[0].ObjectName, "object name mismatch")
			}
			assert.Len(t, ir.Tables, tc.wantTables, "tables count mismatch")
			if tc.wantRaw != "" {
				assert.Equal(t, tc.wantRaw, ir.Tables[0].Raw, "table raw mismatch")
			}
		})
	}
}
