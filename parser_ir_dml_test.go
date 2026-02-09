// parser_ir_dml_test.go exercises INSERT/UPDATE/DELETE specific behaviour.
package postgresparser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIR_AdvancedExpressionsAndSetOps exercises UNION chains plus key DML shapes.
func TestIR_AdvancedExpressionsAndSetOps(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		expectCmd QueryCommand
	}{
		{
			name: "UNION ALL with nested CASE expression",
			sql: `
			(
			  SELECT id,
			         CASE
			           WHEN amount > 100 THEN 'high'
			           ELSE 'low'
			         END AS spending_level
			  FROM orders
			  WHERE status = 'paid'
			)
			UNION ALL
			(
			  SELECT id, 'archived' AS spending_level
			  FROM archived_orders
			)
			ORDER BY id;`,
			expectCmd: QueryCommandSelect,
		},
		{
			name: "INSERT with VALUES and RETURNING",
			sql: `
			INSERT INTO customers (name, email, signup_date)
			VALUES ('John', 'john@example.com', NOW())
			RETURNING id, created_at;`,
			expectCmd: QueryCommandInsert,
		},
		{
			name: "UPDATE with FROM and complex SET expression",
			sql: `
			UPDATE products p
			SET price = price * (1 - d.discount_rate),
			    updated_at = NOW()
			FROM discounts d
			WHERE p.category_id = d.category_id
			  AND d.expires_at > NOW()
			RETURNING p.id, p.price;`,
			expectCmd: QueryCommandUpdate,
		},
		{
			name: "DELETE with USING and RETURNING clause",
			sql: `
			DELETE FROM sessions s
			USING users u
			WHERE s.user_id = u.id AND u.active = false
			RETURNING s.id;`,
			expectCmd: QueryCommandDelete,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ir := parseAssertNoError(t, tc.sql)

			assert.Equal(t, tc.expectCmd, ir.Command, "command mismatch")

			if ir.Command == QueryCommandSelect {
				assert.NotEmpty(t, ir.Columns, "expected columns to be captured")
			}

			if ir.Command == QueryCommandUpdate {
				foundSet := false
				for _, expr := range ir.SetClauses {
					if strings.Contains(expr, "price * (1 - d.discount_rate)") {
						foundSet = true
					}
				}
				assert.True(t, foundSet, "expected complex SET expression, got %+v", ir.SetClauses)
			}

			if ir.Command == QueryCommandDelete {
				assert.NotEmpty(t, ir.Returning, "expected RETURNING clause for DELETE")
			}

			switch tc.name {
			case "UNION ALL with nested CASE expression":
				require.Len(t, ir.SetOperations, 1, "expected one set operation")
				op := ir.SetOperations[0]
				assert.Equal(t, "UNION ALL", op.Type, "expected UNION ALL type")
				require.Len(t, op.Columns, 2, "unexpected union columns")
				assert.Equal(t, "id", op.Columns[0], "unexpected union columns")
				assert.Contains(t, strings.ToLower(op.Query), "from archived_orders", "expected RHS query to reference archived_orders")
				require.Len(t, op.Tables, 1, "expected archived_orders table in set operation")
				assert.Equal(t, "archived_orders", strings.ToLower(op.Tables[0].Name), "expected archived_orders table")
			default:
				assert.Empty(t, ir.SetOperations, "expected no set operations")
			}
		})
	}
}

// TestIR_ExampleLibraryScenarios enforces behaviour from the design examples.
func TestIR_ExampleLibraryScenarios(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{
			name: "Example A - Select with parameter",
			sql:  `SELECT * FROM accounts WHERE status = ?`,
		},
		{
			name: "Example B - Join with explicit predicates",
			sql: `
SELECT orders.id, customers.name
FROM orders
JOIN customers ON customers.id = orders.customer_id`,
		},
		{
			name: "Example C - CTE with window and predicate",
			sql: `
WITH ranked AS (
  SELECT id, RANK() OVER (ORDER BY created_at) AS r
  FROM items
)
SELECT *
FROM ranked
WHERE r <= 10`,
		},
		{
			name: "Example D - Update with returning",
			sql:  `UPDATE products SET price = price * 1.1 WHERE category = ? RETURNING id`,
		},
		{
			name: "Example E - Delete using join and returning",
			sql: `
DELETE FROM sessions
USING users
WHERE sessions.user_id = users.id AND users.active = false
RETURNING sessions.id`,
		},
		{
			name: "Example F - Insert with returning",
			sql:  `INSERT INTO metrics (id, payload) VALUES (?, ?) RETURNING id`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ir := parseAssertNoError(t, tc.sql)

			switch tc.name {
			case "Example A - Select with parameter":
				assert.Equal(t, QueryCommandSelect, ir.Command, "expected SELECT command")
				require.Len(t, ir.Tables, 1, "expected 1 table")
				assert.Equal(t, "accounts", ir.Tables[0].Name, "expected accounts table")
				require.Len(t, ir.Columns, 1, "expected 1 column")
				assert.Equal(t, "*", ir.Columns[0].Expression, "expected wildcard column")
				require.Len(t, ir.Where, 1, "expected WHERE clause")
				assert.Contains(t, normalise(ir.Where[0]), "status=?", "expected status predicate in WHERE")
				require.Len(t, ir.Parameters, 1, "expected single anonymous parameter")
				assert.Equal(t, "?", ir.Parameters[0].Raw, "expected parameter raw value")
				assert.Equal(t, 1, ir.Parameters[0].Position, "expected parameter position")
			case "Example B - Join with explicit predicates":
				assert.Equal(t, QueryCommandSelect, ir.Command, "expected SELECT command")
				require.Len(t, ir.Tables, 2, "expected two tables")
				assert.Equal(t, "orders", ir.Tables[0].Name, "unexpected table 1")
				assert.Equal(t, "customers", ir.Tables[1].Name, "unexpected table 2")
				require.Len(t, ir.JoinConditions, 1, "expected join predicate")
				assert.Contains(t, normalise(ir.JoinConditions[0]), "customers.id=orders.customer_id", "expected join predicate")
			case "Example C - CTE with window and predicate":
				require.Len(t, ir.CTEs, 1, "expected 1 CTE")
				assert.Equal(t, "ranked", strings.ToLower(ir.CTEs[0].Name), "expected ranked CTE")
				// Should now have both the base table "items" and the CTE reference "ranked"
				require.Len(t, ir.Tables, 2, "expected 2 tables (items from CTE and ranked ref)")
				foundItems := false
				foundRanked := false
				for _, tbl := range ir.Tables {
					if tbl.Name == "items" && tbl.Type == TableTypeBase {
						foundItems = true
					}
					if strings.ToLower(tbl.Name) == "ranked" && tbl.Type == TableTypeCTE {
						foundRanked = true
					}
				}
				assert.True(t, foundItems, "expected items base table from CTE")
				assert.True(t, foundRanked, "expected ranked CTE reference")
				require.Len(t, ir.Where, 1, "expected WHERE clause")
				assert.Contains(t, normalise(ir.Where[0]), "r<=10", "expected r <= 10 predicate")
			case "Example D - Update with returning":
				assert.Equal(t, QueryCommandUpdate, ir.Command, "expected UPDATE command")
				require.Len(t, ir.Tables, 1, "expected products table")
				assert.Equal(t, "products", ir.Tables[0].Name, "expected products table")
				foundSet := false
				for _, clause := range ir.SetClauses {
					if strings.Contains(normalise(clause), "price=price*1.1") {
						foundSet = true
						break
					}
				}
				assert.True(t, foundSet, "expected price set clause")
				require.Len(t, ir.Returning, 1, "expected RETURNING clause")
				assert.Contains(t, strings.ToUpper(ir.Returning[0]), "RETURNING", "expected RETURNING clause")
			case "Example E - Delete using join and returning":
				assert.Equal(t, QueryCommandDelete, ir.Command, "expected DELETE command")
				hasUsers := false
				for _, tbl := range ir.Tables {
					if tbl.Name == "users" {
						hasUsers = true
						break
					}
				}
				assert.True(t, hasUsers, "expected users in USING clause")
				require.Len(t, ir.Returning, 1, "expected RETURNING clause")
				assert.Contains(t, ir.Returning[0], "sessions.id", "expected RETURNING sessions.id")
			case "Example F - Insert with returning":
				assert.Equal(t, QueryCommandInsert, ir.Command, "expected INSERT command")
				require.Len(t, ir.InsertColumns, 2, "unexpected insert columns count")
				assert.Equal(t, "id", ir.InsertColumns[0], "unexpected insert column 1")
				assert.Equal(t, "payload", ir.InsertColumns[1], "unexpected insert column 2")
				require.Len(t, ir.Parameters, 2, "expected two anonymous parameters")
				assert.Equal(t, "?", ir.Parameters[0].Raw, "expected parameter 1")
				assert.Equal(t, "?", ir.Parameters[1].Raw, "expected parameter 2")
				require.Len(t, ir.Returning, 1, "expected RETURNING clause")
				assert.Contains(t, ir.Returning[0], "RETURNING id", "expected RETURNING id clause")
			default:
				t.Fatalf("unhandled case %q", tc.name)
			}
		})
	}
}

// TestIR_InsertOnConflictCapturesSetClauses validates DO UPDATE metadata.
func TestIR_InsertOnConflictCapturesSetClauses(t *testing.T) {
	sql := `
INSERT INTO account_balances (id, balance)
VALUES ($1, $2)
ON CONFLICT (id) DO UPDATE
SET balance = EXCLUDED.balance + $3,
    updated_at = NOW()
RETURNING id, balance`

	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandInsert, ir.Command, "expected INSERT command")
	require.Len(t, ir.InsertColumns, 2, "unexpected insert columns count")
	assert.Equal(t, "id", ir.InsertColumns[0], "unexpected insert column 1")
	assert.Equal(t, "balance", ir.InsertColumns[1], "unexpected insert column 2")
	require.NotNil(t, ir.Upsert, "expected upsert metadata")
	assert.Equal(t, "DO UPDATE", ir.Upsert.Action, "expected DO UPDATE action")
	require.Len(t, ir.Upsert.TargetColumns, 1, "unexpected upsert target columns count")
	assert.Equal(t, "id", ir.Upsert.TargetColumns[0], "unexpected upsert target column")
	assert.GreaterOrEqual(t, len(ir.Upsert.SetClauses), 2, "expected set clauses for ON CONFLICT")
	require.Len(t, ir.Parameters, 3, "unexpected parameters count")
	assert.Equal(t, 1, ir.Parameters[0].Position, "unexpected parameter position")
	assert.Equal(t, 3, ir.Parameters[2].Position, "unexpected parameter position")
	require.Len(t, ir.Returning, 1, "expected RETURNING clause")
	assert.Contains(t, ir.Returning[0], "RETURNING id, balance", "expected RETURNING id, balance clause")
}

// TestIR_InsertOnConflictDoNothingMetadata verifies DO NOTHING targets/filters.
func TestIR_InsertOnConflictDoNothingMetadata(t *testing.T) {
	sql := `
INSERT INTO accounts (id, email)
VALUES ($1, $2)
ON CONFLICT (id) WHERE is_active = TRUE DO NOTHING`

	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandInsert, ir.Command, "expected INSERT command")
	require.NotNil(t, ir.Upsert, "expected upsert metadata")
	assert.Equal(t, "DO NOTHING", ir.Upsert.Action, "expected DO NOTHING action")
	require.Len(t, ir.Upsert.TargetColumns, 1, "unexpected target columns count")
	assert.Equal(t, "id", ir.Upsert.TargetColumns[0], "unexpected target column")
	assert.Contains(t, ir.Upsert.TargetWhere, "is_active", "expected target WHERE clause to capture predicate")
	assert.Empty(t, ir.Upsert.SetClauses, "expected no set clauses for DO NOTHING")
}
