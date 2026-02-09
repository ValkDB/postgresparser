// parser_ir_merge_test.go verifies MERGE support and nested subquery metadata.
package postgresparser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIR_MergeStatement validates MERGE target/source and action capture.
func TestIR_MergeStatement(t *testing.T) {
	sql := `
MERGE INTO customers c
USING (
  SELECT customer_id, total_spent
  FROM summary
) s
ON c.id = s.customer_id
WHEN MATCHED AND s.total_spent > 1000 THEN UPDATE SET vip = true
WHEN NOT MATCHED THEN INSERT (id, total_spent) VALUES (s.customer_id, s.total_spent)
WHEN MATCHED THEN DELETE;`

	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandMerge, ir.Command, "expected MERGE command")
	require.NotNil(t, ir.Merge, "expected merge metadata")

	assert.Equal(t, "customers", strings.ToLower(ir.Merge.Target.Name), "unexpected merge target name")
	assert.Equal(t, "c", ir.Merge.Target.Alias, "unexpected merge target alias")

	assert.Equal(t, TableTypeSubquery, ir.Merge.Source.Table.Type, "expected subquery source type")
	assert.Equal(t, "s", ir.Merge.Source.Table.Alias, "expected subquery source alias")

	assert.True(t, containsTable(ir.Tables, "customers"), "expected merge tables to include customers")
	assert.True(t, containsTable(ir.Tables, "summary"), "expected merge tables to include summary")

	assert.Contains(t, ir.Merge.Condition, "c.id = s.customer_id", "unexpected merge condition")

	require.Len(t, ir.Merge.Actions, 3, "expected three merge actions")

	// Action 1: UPDATE
	assert.Equal(t, "UPDATE", ir.Merge.Actions[0].Type, "unexpected update action type")
	assert.Contains(t, ir.Merge.Actions[0].Condition, "total_spent > 1000", "unexpected update condition")
	require.Len(t, ir.Merge.Actions[0].SetClauses, 1, "unexpected update set clauses count")
	assert.Contains(t, ir.Merge.Actions[0].SetClauses[0], "vip = true", "unexpected update set clause")

	// Action 2: INSERT
	assert.Equal(t, "INSERT", ir.Merge.Actions[1].Type, "unexpected insert action type")
	assert.Len(t, ir.Merge.Actions[1].InsertColumns, 2, "unexpected insert columns count")
	assert.Contains(t, ir.Merge.Actions[1].InsertValues, "VALUES (s.customer_id, s.total_spent)", "unexpected insert values")

	// Action 3: DELETE
	assert.Equal(t, "DELETE", ir.Merge.Actions[2].Type, "expected delete action")

	// Check top-level IR SetClauses (should aggregate from actions)
	require.NotEmpty(t, ir.SetClauses, "expected merge set clauses to be recorded")
	assert.Contains(t, ir.SetClauses[0], "vip = true", "expected aggregated set clause")

	// Check Subqueries metadata
	require.NotEmpty(t, ir.Subqueries, "expected merge source subquery metadata")
	assert.Equal(t, "s", strings.ToLower(ir.Subqueries[0].Alias), "expected subquery alias 's'")
	require.NotNil(t, ir.Subqueries[0].Query, "expected parsed subquery")
	assert.True(t, containsTable(ir.Subqueries[0].Query.Tables, "summary"), "expected subquery to capture summary table")
}

// TestExtractDeleteConditionFromText verifies the text-based condition extraction for
// MERGE DELETE clauses. Once the grammar is regenerated with (AND a_expr)? support,
// this helper will be superseded by AST-based extraction via ctx.A_expr().
func TestExtractDeleteConditionFromText(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"WHEN MATCHED THEN DELETE", ""},
		{"WHEN MATCHED DELETE", ""},
		{"WHEN MATCHED AND s.total > 100 THEN DELETE", "s.total > 100"},
		{"WHEN MATCHED AND s.total > 100 DELETE", "s.total > 100"},
		{"WHEN MATCHED AND x = 1 AND y = 2 THEN DELETE", "x = 1 AND y = 2"},
	}
	for _, tt := range tests {
		got := extractDeleteConditionFromText(tt.input)
		assert.Equal(t, tt.want, got, "extractDeleteConditionFromText(%q)", tt.input)
	}
}

// TestIR_SubqueryMetadataFromClause confirms subquery IR is retained.
func TestIR_SubqueryMetadataFromClause(t *testing.T) {
	sql := `
SELECT t.id, sq.total
FROM tenants t
JOIN (
  SELECT tenant_id, SUM(amount) AS total
  FROM invoices
  GROUP BY tenant_id
) sq ON sq.tenant_id = t.id`

	ir := parseAssertNoError(t, sql)

	assert.True(t, containsTable(ir.Tables, "tenants"), "expected tenants table")
	assert.True(t, containsTable(ir.Tables, "invoices"), "expected invoices table")

	foundSubquery := false
	for _, tbl := range ir.Tables {
		if tbl.Type == TableTypeSubquery && strings.ToLower(tbl.Alias) == "sq" {
			foundSubquery = true
			break
		}
	}
	assert.True(t, foundSubquery, "expected subquery table reference in tables list")

	require.NotEmpty(t, ir.Subqueries, "expected subquery metadata")

	var sq *SubqueryRef
	for i := range ir.Subqueries {
		if strings.ToLower(ir.Subqueries[i].Alias) == "sq" {
			sq = &ir.Subqueries[i]
			break
		}
	}
	require.NotNil(t, sq, "expected subquery ref 'sq'")
	require.NotNil(t, sq.Query, "expected parsed subquery")

	assert.True(t, containsTable(sq.Query.Tables, "invoices"), "expected subquery to reference invoices")
	require.NotEmpty(t, sq.Query.GroupBy, "expected subquery group by metadata")
	assert.Contains(t, strings.ToLower(sq.Query.GroupBy[0]), "tenant_id", "expected group by tenant_id")
}
