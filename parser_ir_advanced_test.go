package postgresparser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIR_AdvancedSQLConstructs(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		expectCmd QueryCommand
		check     func(t *testing.T, ir *ParsedQuery)
	}{
		{
			name: "UPSERT (INSERT ... ON CONFLICT)",
			sql: `INSERT INTO users (id, name, email)
			VALUES (1, 'John Doe', 'john.doe@example.com')
			ON CONFLICT (id) DO UPDATE SET
			  name = EXCLUDED.name,
			  email = EXCLUDED.email;`,
			expectCmd: QueryCommandInsert,
			check: func(t *testing.T, ir *ParsedQuery) {
				require.NotNil(t, ir.Upsert, "expected Upsert info for ON CONFLICT DO UPDATE")
				require.NotEmpty(t, ir.Upsert.SetClauses, "expected SetClauses for ON CONFLICT DO UPDATE")
				assert.Contains(t, ir.Upsert.SetClauses[0], "name = EXCLUDED.name", "unexpected set clause")
			},
		},
		{
			name: "UNION",
			sql: `SELECT id, name FROM active_users
			UNION
			SELECT id, name FROM inactive_users;`,
			expectCmd: QueryCommandSelect,
			check: func(t *testing.T, ir *ParsedQuery) {
				require.NotEmpty(t, ir.SetOperations, "expected SetOperations for UNION")
				assert.True(t, containsTable(ir.SetOperations[0].Tables, "inactive_users"), "expected inactive_users in Tables for UNION")
			},
		},
		{
			name: "INTERSECT",
			sql: `SELECT product_id FROM online_orders
			INTERSECT
			SELECT product_id FROM in_store_purchases;`,
			expectCmd: QueryCommandSelect,
			check: func(t *testing.T, ir *ParsedQuery) {
				require.NotEmpty(t, ir.SetOperations, "expected SetOperations for INTERSECT")
				assert.True(t, containsTable(ir.SetOperations[0].Tables, "in_store_purchases"), "expected in_store_purchases in Tables for INTERSECT")
			},
		},
		{
			name: "EXCEPT",
			sql: `SELECT customer_id FROM all_customers
			EXCEPT
			SELECT customer_id FROM recent_buyers;`,
			expectCmd: QueryCommandSelect,
			check: func(t *testing.T, ir *ParsedQuery) {
				require.NotEmpty(t, ir.SetOperations, "expected SetOperations for EXCEPT")
				assert.True(t, containsTable(ir.SetOperations[0].Tables, "recent_buyers"), "expected recent_buyers in Tables for EXCEPT")
			},
		},
		{
			name: "EXISTS Subquery",
			sql: `SELECT name
			FROM customers c
			WHERE EXISTS (
			  SELECT 1
			  FROM orders o
			  WHERE o.customer_id = c.id
			);`,
			expectCmd: QueryCommandSelect,
			check: func(t *testing.T, ir *ParsedQuery) {
				require.NotEmpty(t, ir.Where, "expected WHERE clause for EXISTS")
				assert.True(t, strings.Contains(ir.Where[0], "EXISTS"), "unexpected WHERE clause: %s", ir.Where[0])
			},
		},
		{
			name: "INSERT ... SELECT",
			sql: `INSERT INTO high_value_orders (order_id, customer_id, total)
			SELECT id, customer_id, order_total
			FROM orders
			WHERE order_total > 1000;`,
			expectCmd: QueryCommandInsert,
			check: func(t *testing.T, ir *ParsedQuery) {
				assert.True(t, containsTable(ir.Tables, "high_value_orders"), "expected high_value_orders table for INSERT ... SELECT")
				assert.Len(t, ir.InsertColumns, 3, "expected 3 insert columns for INSERT ... SELECT")
			},
		},
		{
			name: "RIGHT JOIN",
			sql: `SELECT u.name, p.product_name
			FROM users u
			RIGHT JOIN purchases p ON u.id = p.user_id;`,
			expectCmd: QueryCommandSelect,
			check: func(t *testing.T, ir *ParsedQuery) {
				assert.GreaterOrEqual(t, len(ir.Tables), 2, "expected at least 2 tables for RIGHT JOIN")
			},
		},
		{
			name: "FULL OUTER JOIN",
			sql: `SELECT d.name, e.name
			FROM departments d
			FULL OUTER JOIN employees e ON d.id = e.department_id;`,
			expectCmd: QueryCommandSelect,
			check: func(t *testing.T, ir *ParsedQuery) {
				assert.GreaterOrEqual(t, len(ir.Tables), 2, "expected at least 2 tables for FULL OUTER JOIN")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ir := parseAssertNoError(t, tc.sql)

			assert.Equal(t, tc.expectCmd, ir.Command, "command mismatch")

			if tc.check != nil {
				tc.check(t, ir)
			}
		})
	}
}
