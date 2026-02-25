// parser_ir_helpers_test.go provides shared helpers for parser IR tests.
package postgresparser

import (
	"strings"
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/valkdb/postgresparser/gen"
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

func TestCollectCreateTablePrimaryKeyColumns(t *testing.T) {
	t.Run("empty and nil elements", func(t *testing.T) {
		assert.Empty(t, collectCreateTablePrimaryKeyColumns(nil))
		assert.Empty(t, collectCreateTablePrimaryKeyColumns([]gen.ITableelementContext{nil}))
	})

	t.Run("collects only table level primary key columns", func(t *testing.T) {
		sql := `CREATE TABLE public.accounts (
    "ID" integer,
    tenant_id integer,
    code text,
    UNIQUE (code),
    PRIMARY KEY ("ID", tenant_id)
);`
		tableElems := parseCreateTableElements(t, sql)
		pkCols := collectCreateTablePrimaryKeyColumns(tableElems)

		assert.Len(t, pkCols, 2)
		_, hasQuoted := pkCols["ID"]
		assert.True(t, hasQuoted, "expected quoted PK column to be present")
		_, hasUnquoted := pkCols["tenant_id"]
		assert.True(t, hasUnquoted, "expected unquoted PK column to be present")
		_, hasUniqueOnly := pkCols["code"]
		assert.False(t, hasUniqueOnly, "expected UNIQUE-only column to be absent")
	})
}

func TestExtractCreateTableRelationships(t *testing.T) {
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

	pk, fks := extractCreateTableRelationships(tableElems, state.stream)
	require.NotNil(t, pk)
	assert.Equal(t, &DDLPrimaryKey{
		ConstraintName: "users_pk",
		Columns:        []string{"id"},
	}, pk)
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
	}, fks)
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

// parseCreateTableElements returns CREATE TABLE table elements for helper-level DDL extraction tests.
func parseCreateTableElements(t *testing.T, sql string) []gen.ITableelementContext {
	t.Helper()

	input := antlr.NewInputStream(sql)
	lexer := gen.NewPostgreSQLLexer(input)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	parser := gen.NewPostgreSQLParser(stream)
	parser.BuildParseTrees = true

	root := parser.Root()
	require.NotNil(t, root, "expected non-nil parser root")
	require.NotNil(t, root.Stmtblock(), "expected stmtblock")
	stmtMulti := root.Stmtblock().Stmtmulti()
	require.NotNil(t, stmtMulti, "expected stmtmulti")

	stmts := stmtMulti.AllStmt()
	require.Len(t, stmts, 1, "expected exactly one statement")

	createStmt := stmts[0].Createstmt()
	require.NotNil(t, createStmt, "expected CREATE statement")
	require.NotNil(t, createStmt.Opttableelementlist(), "expected table element list")
	require.NotNil(t, createStmt.Opttableelementlist().Tableelementlist(), "expected table elements")

	return createStmt.Opttableelementlist().Tableelementlist().AllTableelement()
}

// normalise collapses whitespace and lowercases strings for comparison convenience.
func normalise(s string) string {
	compact := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(s), " ", ""), "\n", ""), "\t", "")
	return strings.ToLower(compact)
}
