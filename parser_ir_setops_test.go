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
