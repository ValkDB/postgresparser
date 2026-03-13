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
	require.NotNil(t, act.Constraints.PrimaryKey, "expected primary key metadata")
	assert.Equal(t, &DDLPrimaryKey{
		ConstraintName: "accounts_pk",
		Columns:        []string{"id", "tenant_id"},
	}, act.Constraints.PrimaryKey, "primary key metadata mismatch")
	assert.Empty(t, act.Constraints.ForeignKeys, "foreign keys mismatch")

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
	require.NotNil(t, act.Constraints.PrimaryKey, "expected primary key metadata")
	assert.Equal(t, &DDLPrimaryKey{
		Columns: []string{"id", "tenant_id"},
	}, act.Constraints.PrimaryKey, "primary key metadata mismatch")
	assert.Empty(t, act.Constraints.ForeignKeys, "foreign keys mismatch")

	require.Len(t, ir.Tables, 1, "tables count mismatch")
	assert.Empty(t, ir.Tables[0].Schema, "table schema mismatch")
	assert.Equal(t, "accounts", ir.Tables[0].Name, "table name mismatch")
}

func TestIR_DDL_CreateTable_Relationships_TableConstraints(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer,
    org_id integer,
    region text NOT NULL,
    branch_id integer NOT NULL,
    CONSTRAINT users_pk PRIMARY KEY (id),
    CONSTRAINT users_org_fk FOREIGN KEY (org_id) REFERENCES public.organizations(id),
    CONSTRAINT users_branch_fk FOREIGN KEY (region, branch_id) REFERENCES public.branches(region, branch_id)
);`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.DDLActions, 1, "action count mismatch")
	act := ir.DDLActions[0]
	assert.Equal(t, DDLCreateTable, act.Type, "expected CREATE_TABLE")

	require.NotNil(t, act.Constraints.PrimaryKey, "expected primary key metadata")
	assert.Equal(t, &DDLPrimaryKey{
		ConstraintName: "users_pk",
		Columns:        []string{"id"},
	}, act.Constraints.PrimaryKey, "primary key metadata mismatch")

	assert.Equal(t, []DDLForeignKey{
		{
			ConstraintName:    "users_org_fk",
			Columns:           []string{"org_id"},
			ReferencesSchema:  "public",
			ReferencesTable:   "organizations",
			ReferencesColumns: []string{"id"},
		},
		{
			ConstraintName:    "users_branch_fk",
			Columns:           []string{"region", "branch_id"},
			ReferencesSchema:  "public",
			ReferencesTable:   "branches",
			ReferencesColumns: []string{"region", "branch_id"},
		},
	}, act.Constraints.ForeignKeys, "foreign keys mismatch")

	require.Len(t, act.ColumnDetails, 4, "column details mismatch")
	assert.Equal(t, DDLColumn{Name: "id", Type: "integer", Nullable: false}, act.ColumnDetails[0], "id column mismatch")
}

func TestIR_DDL_CreateTable_Relationships_InlineConstraints(t *testing.T) {
	sql := `CREATE TABLE public.memberships (
    id integer PRIMARY KEY,
    org_id integer CONSTRAINT memberships_org_fk REFERENCES public.organizations(id),
    branch_id integer REFERENCES branches(id)
);`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.DDLActions, 1, "action count mismatch")
	act := ir.DDLActions[0]
	assert.Equal(t, DDLCreateTable, act.Type, "expected CREATE_TABLE")

	require.NotNil(t, act.Constraints.PrimaryKey, "expected primary key metadata")
	assert.Equal(t, &DDLPrimaryKey{
		Columns: []string{"id"},
	}, act.Constraints.PrimaryKey, "primary key metadata mismatch")

	assert.Equal(t, []DDLForeignKey{
		{
			ConstraintName:    "memberships_org_fk",
			Columns:           []string{"org_id"},
			ReferencesSchema:  "public",
			ReferencesTable:   "organizations",
			ReferencesColumns: []string{"id"},
		},
		{
			Columns:           []string{"branch_id"},
			ReferencesTable:   "branches",
			ReferencesColumns: []string{"id"},
		},
	}, act.Constraints.ForeignKeys, "foreign keys mismatch")

	require.Len(t, act.ColumnDetails, 3, "column details mismatch")
	assert.Equal(t, DDLColumn{Name: "id", Type: "integer", Nullable: false}, act.ColumnDetails[0], "id column mismatch")
}

func TestIR_DDL_CreateTable_Relationships_ReferentialActions(t *testing.T) {
	sql := `CREATE TABLE public.orders (
    id integer PRIMARY KEY,
    user_id integer REFERENCES public.users(id) ON DELETE CASCADE ON UPDATE SET NULL,
    product_id integer,
    CONSTRAINT orders_product_fk FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE SET DEFAULT ON UPDATE RESTRICT
);`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.DDLActions, 1, "action count mismatch")
	act := ir.DDLActions[0]

	require.NotNil(t, act.Constraints.PrimaryKey, "expected primary key metadata")
	assert.Equal(t, &DDLPrimaryKey{Columns: []string{"id"}}, act.Constraints.PrimaryKey, "primary key mismatch")

	require.Len(t, act.Constraints.ForeignKeys, 2, "foreign key count mismatch")

	assert.Equal(t, DDLForeignKey{
		Columns:           []string{"user_id"},
		ReferencesSchema:  "public",
		ReferencesTable:   "users",
		ReferencesColumns: []string{"id"},
		OnDelete:          FKCascade,
		OnUpdate:          FKSetNull,
	}, act.Constraints.ForeignKeys[0], "inline FK mismatch")

	assert.Equal(t, DDLForeignKey{
		ConstraintName:    "orders_product_fk",
		Columns:           []string{"product_id"},
		ReferencesTable:   "products",
		ReferencesColumns: []string{"id"},
		OnDelete:          FKSetDefault,
		OnUpdate:          FKRestrict,
	}, act.Constraints.ForeignKeys[1], "table-level FK mismatch")
}

func TestIR_DDL_CreateTable_Relationships_NoAction(t *testing.T) {
	sql := `CREATE TABLE t (
    id integer PRIMARY KEY,
    ref_id integer REFERENCES other(id) ON DELETE NO ACTION
);`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.DDLActions, 1)
	require.Len(t, ir.DDLActions[0].Constraints.ForeignKeys, 1)
	assert.Equal(t, FKNoAction, ir.DDLActions[0].Constraints.ForeignKeys[0].OnDelete)
	assert.Empty(t, ir.DDLActions[0].Constraints.ForeignKeys[0].OnUpdate)
}

func TestIR_DDL_CreateTable_UniqueConstraints(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer PRIMARY KEY,
    email text UNIQUE,
    code text,
    region text,
    CONSTRAINT users_code_region_uniq UNIQUE (code, region)
);`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandDDL, ir.Command)
	require.Len(t, ir.DDLActions, 1)
	act := ir.DDLActions[0]

	assert.Equal(t, []DDLUniqueConstraint{
		{Columns: []string{"email"}},
		{ConstraintName: "users_code_region_uniq", Columns: []string{"code", "region"}},
	}, act.Constraints.UniqueKeys, "unique keys mismatch")
	assert.NotNil(t, act.Constraints.PrimaryKey, "expected primary key")
}

func TestIR_DDL_AlterTableAddConstraintUnique(t *testing.T) {
	sql := `ALTER TABLE public.users ADD CONSTRAINT users_email_uniq UNIQUE (email);`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandDDL, ir.Command)
	require.Len(t, ir.DDLActions, 1)
	act := ir.DDLActions[0]

	assert.Equal(t, DDLAlterTable, act.Type)
	assert.Contains(t, act.Flags, "ADD_CONSTRAINT")
	assert.Equal(t, []DDLUniqueConstraint{
		{ConstraintName: "users_email_uniq", Columns: []string{"email"}},
	}, act.Constraints.UniqueKeys, "unique keys mismatch")
	assert.Nil(t, act.Constraints.PrimaryKey)
	assert.Empty(t, act.Constraints.ForeignKeys)
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
		{
			name: "comment variants with spacing and dollar content",
			sql: `CREATE TABLE public.users (
    --    starts with spaces
    --no-space-prefix
    -- $tag marker
    name text
);`,
			opts: ParseOptions{IncludeCreateTableFieldComments: true},
			wantCommentsByCol: map[string][]string{
				"name": {"starts with spaces", "no-space-prefix", "$tag marker"},
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

func TestExtractCreateTableFieldCommentsByColumn_Table(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want map[string][]string
	}{
		{
			name: "issue25 extraction",
			sql: `CREATE TABLE public.users (
    -- [Attribute("Just an example")]
    -- required, min 5, max 55
    name        text,

    -- single-column FK, inline
    org_id      integer     REFERENCES public.organizations(id)
);`,
			want: map[string][]string{
				"name":   {`[Attribute("Just an example")]`, "required, min 5, max 55"},
				"org_id": {"single-column FK, inline"},
			},
		},
		{
			name: "constraint comments are skipped",
			sql: `CREATE TABLE t (
    -- comment for id
    id integer,
    -- should not bind
    CONSTRAINT t_pk PRIMARY KEY (id),
    -- comment for email
    email text
);`,
			want: map[string][]string{
				"id":    {"comment for id"},
				"email": {"comment for email"},
			},
		},
		{
			name: "quoted and unquoted identifiers normalize differently",
			sql: `CREATE TABLE t (
    -- quoted
    "Email" text,
    -- unquoted
    email text
);`,
			want: map[string][]string{
				"Email": {"quoted"},
				"email": {"unquoted"},
			},
		},
		{
			name: "unquoted uppercase identifier normalizes to lowercase",
			sql: `CREATE TABLE t (
    -- uppercase spelling
    NAME text
);`,
			want: map[string][]string{
				"name": {"uppercase spelling"},
			},
		},
		{
			name: "embedded commas and dollar strings do not break splitting",
			sql: `CREATE TABLE t (
    -- payload docs
    payload text DEFAULT $tag$a,b,c$tag$,
    -- tags docs
    tags text[] DEFAULT ARRAY['x','y']
);`,
			want: map[string][]string{
				"payload": {"payload docs"},
				"tags":    {"tags docs"},
			},
		},
		{
			name: "line comments trim leading spaces and keep dollar text",
			sql: `CREATE TABLE t (
    --    starts with spaces
    --no-space-prefix
    -- $tag marker
    name text
);`,
			want: map[string][]string{
				"name": {"starts with spaces", "no-space-prefix", "$tag marker"},
			},
		},
	}

	t.Run("nil inputs", func(t *testing.T) {
		assert.Nil(t, extractCreateTableFieldCommentsByColumn(nil, nil))
	})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state, err := prepareParseState(tc.sql, false)
			require.NoError(t, err)
			require.Len(t, state.stmts, 1)
			createStmt := state.stmts[0].Createstmt()
			require.NotNil(t, createStmt)
			require.NotNil(t, createStmt.Opttableelementlist())
			require.NotNil(t, createStmt.Opttableelementlist().Tableelementlist())

			tableElems := createStmt.Opttableelementlist().Tableelementlist().AllTableelement()
			got := extractCreateTableFieldCommentsByColumn(tableElems, state.stream)
			assert.Equal(t, tc.want, got, "extracted comments mismatch")
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

	require.Len(t, ir.DDLActions, 1, "action count mismatch")
	act := ir.DDLActions[0]
	assert.Equal(t, DDLAlterTable, act.Type, "expected ALTER_TABLE")
	assert.Equal(t, "public", act.Schema, "action schema mismatch")
	assert.Equal(t, "schema_migrations", act.ObjectName, "action object mismatch")
	assert.Contains(t, act.Flags, "ADD_CONSTRAINT", "expected flag ADD_CONSTRAINT")
	assert.Equal(t, []string{"version"}, act.Columns, "constrained columns mismatch")
	assert.Equal(t, &DDLPrimaryKey{
		ConstraintName: "schema_migrations_pkey",
		Columns:        []string{"version"},
	}, act.Constraints.PrimaryKey, "primary key mismatch")
	assert.Empty(t, act.Constraints.ForeignKeys, "foreign keys mismatch")
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

	require.Len(t, ir.DDLActions, 1, "action count mismatch")
	act := ir.DDLActions[0]
	assert.Equal(t, DDLAlterTable, act.Type, "expected ALTER_TABLE")
	assert.Empty(t, act.Schema, "action schema mismatch")
	assert.Equal(t, "schema_migrations", act.ObjectName, "action object mismatch")
	assert.Contains(t, act.Flags, "ADD_CONSTRAINT", "expected flag ADD_CONSTRAINT")
	assert.Equal(t, []string{"version"}, act.Columns, "constrained columns mismatch")
	assert.Equal(t, &DDLPrimaryKey{
		ConstraintName: "schema_migrations_pkey",
		Columns:        []string{"version"},
	}, act.Constraints.PrimaryKey, "primary key mismatch")
	assert.Empty(t, act.Constraints.ForeignKeys, "foreign keys mismatch")
}

func TestIR_DDL_AlterTableAddConstraintForeignKey(t *testing.T) {
	sql := `ALTER TABLE public.users
    ADD CONSTRAINT users_org_fk FOREIGN KEY (org_id) REFERENCES public.organizations(id);`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.DDLActions, 1, "action count mismatch")

	act := ir.DDLActions[0]
	assert.Equal(t, DDLAlterTable, act.Type, "expected ALTER_TABLE")
	assert.Equal(t, "users", act.ObjectName, "object name mismatch")
	assert.Equal(t, "public", act.Schema, "schema mismatch")
	assert.Contains(t, act.Flags, "ADD_CONSTRAINT", "expected flag ADD_CONSTRAINT")
	assert.Nil(t, act.Constraints.PrimaryKey, "primary key mismatch")
	assert.Equal(t, []string{"org_id"}, act.Columns, "constrained columns mismatch")
	assert.Equal(t, []DDLForeignKey{
		{
			ConstraintName:    "users_org_fk",
			Columns:           []string{"org_id"},
			ReferencesSchema:  "public",
			ReferencesTable:   "organizations",
			ReferencesColumns: []string{"id"},
		},
	}, act.Constraints.ForeignKeys, "foreign keys mismatch")
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

func TestIR_DDL_AlterTableAddConstraintCheck(t *testing.T) {
	tests := []struct {
		name           string
		sql            string
		wantObject     string
		wantSchema     string
		wantConstraint string
		wantExpr       string
	}{
		{
			name:           "simple CHECK",
			sql:            `ALTER TABLE public.products ADD CONSTRAINT positive_price CHECK (price > 0);`,
			wantObject:     "products",
			wantSchema:     "public",
			wantConstraint: "positive_price",
			wantExpr:       "price > 0",
		},
		{
			name:           "CHECK with AND",
			sql:            `ALTER TABLE orders ADD CONSTRAINT valid_qty CHECK (quantity > 0 AND quantity < 10000);`,
			wantObject:     "orders",
			wantConstraint: "valid_qty",
			wantExpr:       "quantity > 0 AND quantity < 10000",
		},
		{
			name:           "CHECK with ONLY",
			sql:            `ALTER TABLE ONLY public.store_settings ADD CONSTRAINT check_name CHECK (name <> '');`,
			wantObject:     "store_settings",
			wantSchema:     "public",
			wantConstraint: "check_name",
			wantExpr:       "name <> ''",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ir := parseAssertNoError(t, tc.sql)

			assert.Equal(t, QueryCommandDDL, ir.Command)
			require.Len(t, ir.DDLActions, 1)
			act := ir.DDLActions[0]

			assert.Equal(t, DDLAlterTable, act.Type)
			assert.Equal(t, tc.wantObject, act.ObjectName)
			assert.Equal(t, tc.wantSchema, act.Schema)
			assert.Contains(t, act.Flags, "ADD_CONSTRAINT")

			require.NotNil(t, act.Constraints)
			require.Len(t, act.Constraints.CheckConstraints, 1)
			check := act.Constraints.CheckConstraints[0]
			assert.Equal(t, tc.wantConstraint, check.ConstraintName)
			assert.Equal(t, tc.wantExpr, check.Expression)

			assert.Nil(t, act.Constraints.PrimaryKey)
			assert.Empty(t, act.Constraints.ForeignKeys)
			assert.Empty(t, act.Constraints.UniqueKeys)
		})
	}
}

func TestIR_DDL_CreateTableWithCheckConstraint(t *testing.T) {
	tests := []struct {
		name           string
		sql            string
		wantChecks     int
		wantConstraint string
		wantExpr       string
	}{
		{
			name: "table-level CHECK",
			sql: `CREATE TABLE products (
				id serial PRIMARY KEY,
				price numeric,
				CONSTRAINT positive_price CHECK (price > 0)
			);`,
			wantChecks:     1,
			wantConstraint: "positive_price",
			wantExpr:       "price > 0",
		},
		{
			name: "inline column CHECK",
			sql: `CREATE TABLE products (
				id serial PRIMARY KEY,
				price numeric CONSTRAINT positive_price CHECK (price > 0)
			);`,
			wantChecks:     1,
			wantConstraint: "positive_price",
			wantExpr:       "price > 0",
		},
		{
			name: "unnamed inline CHECK",
			sql: `CREATE TABLE products (
				id serial PRIMARY KEY,
				price numeric CHECK (price > 0)
			);`,
			wantChecks: 1,
			wantExpr:   "price > 0",
		},
		{
			name: "multiple CHECK constraints",
			sql: `CREATE TABLE products (
				id serial PRIMARY KEY,
				price numeric,
				quantity integer,
				CONSTRAINT positive_price CHECK (price > 0),
				CONSTRAINT valid_qty CHECK (quantity >= 0)
			);`,
			wantChecks: 2,
		},
		{
			name: "issue #32 — pg_dump style with ANY()",
			sql: `CREATE TABLE public.store_settings (
				id boolean DEFAULT true NOT NULL,
				active_languages text[] NOT NULL,
				default_language text NOT NULL,
				max_players_per_team integer NOT NULL,
				timezone text NOT NULL,
				CONSTRAINT store_settings_check CHECK ((default_language = ANY (active_languages))),
				CONSTRAINT store_settings_id_check CHECK ((id = true))
			);`,
			wantChecks:     2,
			wantConstraint: "store_settings_check",
			wantExpr:       "(default_language = ANY (active_languages))",
		},
		{
			name: "CHECK with boolean expression and nested parens",
			sql: `CREATE TABLE public.events (
				start_date date NOT NULL,
				end_date date NOT NULL,
				CONSTRAINT valid_dates CHECK ((end_date > start_date))
			);`,
			wantChecks:     1,
			wantConstraint: "valid_dates",
			wantExpr:       "(end_date > start_date)",
		},
		{
			name: "CHECK with OR and function call",
			sql: `CREATE TABLE public.users (
				email text NOT NULL,
				CONSTRAINT valid_email CHECK ((email ~~ '%@%'::text) OR (length(email) > 5))
			);`,
			wantChecks:     1,
			wantConstraint: "valid_email",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ir := parseAssertNoError(t, tc.sql)

			assert.Equal(t, QueryCommandDDL, ir.Command)
			require.Len(t, ir.DDLActions, 1)
			act := ir.DDLActions[0]
			assert.Equal(t, DDLCreateTable, act.Type)

			require.NotNil(t, act.Constraints)
			require.Len(t, act.Constraints.CheckConstraints, tc.wantChecks)

			if tc.wantConstraint != "" {
				assert.Equal(t, tc.wantConstraint, act.Constraints.CheckConstraints[0].ConstraintName)
			}
			if tc.wantExpr != "" {
				assert.Equal(t, tc.wantExpr, act.Constraints.CheckConstraints[0].Expression)
			}
		})
	}
}

// TestIR_DDL_Issue32_StoreSettingsCheck validates the exact SQL from issue #32.
func TestIR_DDL_Issue32_StoreSettingsCheck(t *testing.T) {
	sql := `CREATE TABLE public.store_settings (
    id boolean DEFAULT true NOT NULL,
    active_languages text[] NOT NULL,
    default_language text NOT NULL,
    max_players_per_team integer NOT NULL,
    timezone text NOT NULL,
    CONSTRAINT store_settings_check CHECK ((default_language = ANY (active_languages))),
    CONSTRAINT store_settings_id_check CHECK ((id = true))
);`

	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandDDL, ir.Command)
	require.Len(t, ir.DDLActions, 1)
	act := ir.DDLActions[0]

	assert.Equal(t, DDLCreateTable, act.Type)
	assert.Equal(t, "store_settings", act.ObjectName)
	assert.Equal(t, "public", act.Schema)

	// Verify columns
	require.Len(t, act.ColumnDetails, 5)
	assert.Equal(t, "id", act.ColumnDetails[0].Name)
	assert.Equal(t, "boolean", act.ColumnDetails[0].Type)
	assert.Equal(t, false, act.ColumnDetails[0].Nullable)
	assert.Equal(t, "true", act.ColumnDetails[0].Default)
	assert.Equal(t, "text[]", act.ColumnDetails[1].Type)

	// Verify both CHECK constraints
	require.NotNil(t, act.Constraints)
	require.Len(t, act.Constraints.CheckConstraints, 2)

	assert.Equal(t, "store_settings_check", act.Constraints.CheckConstraints[0].ConstraintName)
	assert.Equal(t, "(default_language = ANY (active_languages))", act.Constraints.CheckConstraints[0].Expression)

	assert.Equal(t, "store_settings_id_check", act.Constraints.CheckConstraints[1].ConstraintName)
	assert.Equal(t, "(id = true)", act.Constraints.CheckConstraints[1].Expression)
}
