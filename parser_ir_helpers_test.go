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

// normalise collapses whitespace and lowercases strings for comparison convenience.
func normalise(s string) string {
	compact := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(s), " ", ""), "\n", ""), "\t", "")
	return strings.ToLower(compact)
}
