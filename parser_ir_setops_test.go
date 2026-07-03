// parser_ir_setops_test.go focuses on UNION/INTERSECT/EXCEPT metadata.
package postgresparser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIR_SetOperationsMetadata checks multi-branch set-ops are captured in order.
func TestIR_SetOperationsMetadata(t *testing.T) {
	sql := `
(
  SELECT user_id
  FROM logins
  INTERSECT
  SELECT user_id
  FROM payments
)
EXCEPT ALL
SELECT user_id
FROM banned_users;`

	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.SetOperations, 2, "expected two set operations")

	first := ir.SetOperations[0]
	assert.Equal(t, "INTERSECT", first.Type, "expected first operation INTERSECT")
	require.Len(t, first.Columns, 1, "unexpected INTERSECT columns count")
	assert.Equal(t, "user_id", first.Columns[0], "unexpected INTERSECT column")
	assert.Contains(t, strings.ToLower(first.Query), "from payments", "expected INTERSECT query to contain payments")
	require.Len(t, first.Tables, 1, "expected payments table captured for INTERSECT")
	assert.Equal(t, "payments", strings.ToLower(first.Tables[0].Name), "expected payments table captured for INTERSECT")

	second := ir.SetOperations[1]
	assert.Equal(t, "EXCEPT ALL", second.Type, "expected second operation EXCEPT ALL")
	require.Len(t, second.Columns, 1, "unexpected EXCEPT columns count")
	assert.Equal(t, "user_id", second.Columns[0], "unexpected EXCEPT column")
	assert.Contains(t, strings.ToLower(second.Query), "from banned_users", "expected EXCEPT query to contain banned_users")
	require.Len(t, second.Tables, 1, "expected banned_users table captured for EXCEPT")
	assert.Equal(t, "banned_users", strings.ToLower(second.Tables[0].Name), "expected banned_users table captured for EXCEPT")

	assert.True(t, containsTable(ir.Tables, "logins"), "expected logins table in top level")
	assert.True(t, containsTable(ir.Tables, "payments"), "expected payments table in top level")
	assert.True(t, containsTable(ir.Tables, "banned_users"), "expected banned_users table in top level")
}

// TestIR_SetOperationTableDriven sanity-checks table capture across common set ops.
func TestIR_SetOperationTableDriven(t *testing.T) {
	tests := []struct {
		name             string
		sql              string
		expectedTables   []string
		expectedOpTables []string
	}{
		{
			name: "Union distinct across staging tables",
			sql: `
SELECT account_id FROM current_accounts
UNION
SELECT account_id FROM archived_accounts`,
			expectedTables:   []string{"current_accounts", "archived_accounts"},
			expectedOpTables: []string{"archived_accounts"},
		},
		{
			name: "Intersect dedupe check",
			sql: `
SELECT user_id FROM service_a_events
INTERSECT ALL
SELECT user_id FROM service_b_events`,
			expectedTables:   []string{"service_a_events", "service_b_events"},
			expectedOpTables: []string{"service_b_events"},
		},
		{
			name: "Except eliminates revoked permissions",
			sql: `
SELECT user_id FROM granted_permissions
EXCEPT
SELECT user_id FROM revoked_permissions`,
			expectedTables:   []string{"granted_permissions", "revoked_permissions"},
			expectedOpTables: []string{"revoked_permissions"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ir := parseAssertNoError(t, tc.sql)

			require.NotEmpty(t, ir.SetOperations, "expected set operations metadata")

			for _, tbl := range tc.expectedTables {
				assert.True(t, containsTable(ir.Tables, tbl), "expected table %q in top-level tables", tbl)
			}

			for _, tbl := range tc.expectedOpTables {
				found := false
				for _, op := range ir.SetOperations {
					if containsTable(op.Tables, tbl) {
						found = true
						break
					}
				}
				assert.True(t, found, "expected table %q in set operations", tbl)
			}
		})
	}
}

// TestIR_SetOpRHSWhereColumnUsage checks right-hand set-operation branches
// record their WHERE filters exactly once; INTERSECT used to drop them.
func TestIR_SetOpRHSWhereColumnUsage(t *testing.T) {
	for _, op := range []string{"UNION", "INTERSECT", "EXCEPT"} {
		t.Run(op, func(t *testing.T) {
			ir := parseAssertNoError(t, "SELECT a FROM t1 WHERE x = 1 "+op+" SELECT a FROM t2 WHERE y = 2")

			count := 0
			for _, usage := range ir.ColumnUsage {
				if usage.UsageType == ColumnUsageTypeFilter && strings.EqualFold(usage.Column, "y") {
					count++
				}
			}
			assert.Equal(t, 1, count, "expected RHS filter column y recorded exactly once")
		})
	}
}

// TestIR_SetOpBranchLimitIsNested guards the isNested flag propagation through
// the consolidated populateSelectFromResolved entry-point: a LIMIT on the
// top-level select must report IsNested=false, while a LIMIT inside a
// set-operation branch must report IsNested=true.
func TestIR_SetOpBranchLimitIsNested(t *testing.T) {
	sql := `
(SELECT id FROM a)
UNION ALL
(SELECT id FROM b LIMIT 5)
LIMIT 10`

	ir := parseAssertNoError(t, sql)

	require.NotNil(t, ir.Limit, "expected top-level LIMIT")
	assert.Contains(t, ir.Limit.Limit, "10", "unexpected top-level LIMIT value")
	assert.False(t, ir.Limit.IsNested, "expected IsNested=false on top-level LIMIT")

	require.Len(t, ir.SetOperations, 1, "expected one set operation")
	require.NotEmpty(t, ir.Subqueries, "expected set-op branch captured as subquery")

	var branch *ParsedQuery
	for _, sq := range ir.Subqueries {
		if sq.Query != nil && sq.Query.Limit != nil {
			branch = sq.Query
			break
		}
	}
	require.NotNil(t, branch, "expected a set-op branch ParsedQuery with a LIMIT")
	assert.Contains(t, branch.Limit.Limit, "5", "unexpected branch LIMIT value")
	assert.True(t, branch.Limit.IsNested, "expected IsNested=true on set-op branch LIMIT")
}
