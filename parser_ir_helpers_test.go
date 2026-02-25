// parser_ir_helpers_test.go provides shared helpers for parser IR tests.
package postgresparser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parseAssertNoError parses SQL and fails the test if an error occurs.
func parseAssertNoError(t *testing.T, sql string) *ParsedQuery {
	t.Helper()
	q, err := ParseSQL(sql)
	require.NoError(t, err, "ParseSQL(%q) returned error", sql)
	require.NotNil(t, q, "ParseSQL(%q) returned nil query", sql)
	return q
}

// containsTable reports whether a table list includes the supplied name (case-insensitive).
func containsTable(tables []TableRef, name string) bool {
	target := strings.ToLower(name)
	for _, tbl := range tables {
		if strings.ToLower(tbl.Name) == target {
			return true
		}
	}
	return false
}

// TestSplitQualifiedName verifies schema/name splitting including quoted identifiers.
func TestSplitQualifiedName(t *testing.T) {
	tests := []struct {
		input      string
		wantSchema string
		wantName   string
	}{
		{"", "", ""},
		{"users", "", "users"},
		{"public.users", "public", "users"},
		{"mydb.public.users", "mydb.public", "users"},
		{`"my.schema"."my.table"`, `"my.schema"`, `"my.table"`},
		{`"dotted.schema".simple_table`, `"dotted.schema"`, "simple_table"},
		{`simple_schema."dotted.table"`, "simple_schema", `"dotted.table"`},
	}
	for _, tt := range tests {
		schema, name := splitQualifiedName(tt.input)
		assert.Equal(t, tt.wantSchema, schema, "schema mismatch for input %q", tt.input)
		assert.Equal(t, tt.wantName, name, "name mismatch for input %q", tt.input)
	}
}

func TestNormalizeCreateTableColumnName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "empty string",
			input: "   ",
			want:  "",
		},
		{
			name:  "unquoted folded to lowercase",
			input: "Tenant_ID",
			want:  "tenant_id",
		},
		{
			name:  "quoted keeps case",
			input: `"Tenant_ID"`,
			want:  "Tenant_ID",
		},
		{
			name:  "quoted escaped quote is unescaped",
			input: `"A""B"`,
			want:  `A"B`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, normalizeCreateTableColumnName(tc.input))
		})
	}
}

func TestCreateTablePrimaryKeyColumnSet(t *testing.T) {
	t.Run("nil primary key", func(t *testing.T) {
		assert.Empty(t, createTablePrimaryKeyColumnSet(nil))
	})

	t.Run("empty columns", func(t *testing.T) {
		assert.Empty(t, createTablePrimaryKeyColumnSet(&DDLPrimaryKey{}))
	})

	t.Run("normalizes quoted and unquoted columns", func(t *testing.T) {
		pk := &DDLPrimaryKey{
			ConstraintName: "accounts_pk",
			Columns:        []string{`"ID"`, "tenant_id", "Uppercase"},
		}
		pkCols := createTablePrimaryKeyColumnSet(pk)

		assert.Len(t, pkCols, 3)
		_, hasQuoted := pkCols["ID"]
		assert.True(t, hasQuoted, "expected quoted PK column to preserve case")
		_, hasUnquoted := pkCols["tenant_id"]
		assert.True(t, hasUnquoted, "expected unquoted PK column to be lowercased")
		_, hasUppercase := pkCols["uppercase"]
		assert.True(t, hasUppercase, "expected unquoted uppercase PK column to be lowercased")
	})
}

func TestExtractCreateTableConstraints(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer,
    org_id integer CONSTRAINT users_org_fk REFERENCES public.organizations(id),
    region text,
    branch_id integer,
    CONSTRAINT users_pk PRIMARY KEY (id),
    CONSTRAINT users_branch_fk FOREIGN KEY (region, branch_id) REFERENCES public.branches(region, branch_id)
);`

	state, err := prepareParseState(sql, false)
	require.NoError(t, err)
	require.Len(t, state.stmts, 1)

	createStmt := state.stmts[0].Createstmt()
	require.NotNil(t, createStmt)
	require.NotNil(t, createStmt.Opttableelementlist())
	require.NotNil(t, createStmt.Opttableelementlist().Tableelementlist())
	tableElems := createStmt.Opttableelementlist().Tableelementlist().AllTableelement()

	constraints := extractCreateTableConstraints(tableElems, state.stream)
	require.NotNil(t, constraints.PrimaryKey)
	assert.Equal(t, &DDLPrimaryKey{
		ConstraintName: "users_pk",
		Columns:        []string{"id"},
	}, constraints.PrimaryKey)
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
	}, constraints.ForeignKeys)
	assert.Empty(t, constraints.UniqueKeys)
}

func TestCollectAlterTableConstraintColumns(t *testing.T) {
	pk := &DDLPrimaryKey{
		Columns: []string{"id", `"CaseSensitive"`},
	}
	fks := []DDLForeignKey{
		{Columns: []string{"org_id"}},
		{Columns: []string{"id", `"CaseSensitive"`, "branch_id"}},
	}

	got := collectAlterTableConstraintColumns(pk, fks)
	assert.Equal(t, []string{"id", `"CaseSensitive"`, "org_id", "branch_id"}, got)
}

// normalise collapses whitespace and lowercases strings for comparison convenience.
func normalise(s string) string {
	compact := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(s), " ", ""), "\n", ""), "\t", "")
	return strings.ToLower(compact)
}
