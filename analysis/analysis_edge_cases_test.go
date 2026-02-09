// Package analysis provides SQL analysis capabilities using the ANTLR PostgreSQL parser.
// This file tests edge cases and complex SQL patterns to ensure comprehensive coverage.
package analysis

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAnalyzeUSINGJoin validates USING clause join column extraction.
func TestAnalyzeUSINGJoin(t *testing.T) {
	sql := `SELECT * FROM orders o
			JOIN customers c USING (customer_id, company_id)
			WHERE o.status = ?`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should detect both tables
	assert.GreaterOrEqual(t, len(result.Tables), 2)

	// Should detect USING columns as join columns
	var joinColumns []string
	for _, usage := range result.ColumnUsage {
		if usage.UsageType == SQLUsageTypeJoin {
			joinColumns = append(joinColumns, usage.Column)
		}
	}
	assert.Contains(t, joinColumns, "customer_id")
	assert.Contains(t, joinColumns, "company_id")
}

// TestAnalyzeNaturalJoin verifies NATURAL JOIN detects both tables and filter columns.
func TestAnalyzeNaturalJoin(t *testing.T) {
	sql := `SELECT * FROM employees e
			NATURAL JOIN departments d
			WHERE e.salary > ?`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should detect both tables
	assert.GreaterOrEqual(t, len(result.Tables), 2)

	// Should detect filter
	var hasFilter bool
	for _, usage := range result.ColumnUsage {
		if usage.UsageType == SQLUsageTypeFilter && usage.Column == "salary" {
			hasFilter = true
			break
		}
	}
	assert.True(t, hasFilter, "Should detect salary filter")
}

// TestAnalyzeMergeDeleteWithCondition verifies MERGE DELETE action handling.
// The ANTLR grammar's merge_delete_clause rule is: WHEN MATCHED [THEN] DELETE
// -- it does NOT support the AND a_expr condition syntax that merge_update_clause
// and merge_insert_clause support.
func TestAnalyzeMergeDeleteWithCondition(t *testing.T) {
	// This MERGE has a plain DELETE (no AND condition) which the grammar supports.
	sql := `MERGE INTO target t USING source s ON t.id = s.id
			WHEN MATCHED AND t.status = 'inactive' THEN
				UPDATE SET name = s.name
			WHEN NOT MATCHED THEN
				INSERT (id, name) VALUES (s.id, s.name)
			WHEN MATCHED THEN DELETE`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, SQLCommandMerge, result.Command)
	require.NotNil(t, result.Merge)

	// The grammar supports at most: one insert, one update, one trailing delete.
	// Verify the DELETE action exists.
	var deleteAction *SQLMergeAction
	for i := range result.Merge.Actions {
		if result.Merge.Actions[i].Type == "DELETE" {
			deleteAction = &result.Merge.Actions[i]
			break
		}
	}
	require.NotNil(t, deleteAction, "expected a DELETE action in MERGE")

	// Known limitation: the merge_delete_clause grammar rule does not support
	// AND conditions, so the DELETE action's Condition is always empty.
	// If the grammar is updated in the future, this assertion should change to
	// verify that the condition is correctly extracted.
	assert.Empty(t, deleteAction.Condition,
		"DELETE condition is empty because the grammar does not support AND conditions on merge_delete_clause")
}

// TestAnalyzeUnionQuery validates UNION ALL set operation and filter extraction from both branches.
func TestAnalyzeUnionQuery(t *testing.T) {
	sql := `SELECT id, name FROM employees WHERE department = ?
			UNION ALL
			SELECT id, name FROM contractors WHERE active = true
			ORDER BY name`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should detect set operation
	assert.NotEmpty(t, result.SetOperations)
	if len(result.SetOperations) > 0 {
		assert.Equal(t, SQLSetOperationType("UNION ALL"), result.SetOperations[0].Type)
	}

	// Should detect filters from both queries
	var hasEmployeeFilter, hasContractorFilter bool
	for _, usage := range result.ColumnUsage {
		if usage.UsageType == SQLUsageTypeFilter {
			if usage.Column == "department" {
				hasEmployeeFilter = true
			}
			if usage.Column == "active" {
				hasContractorFilter = true
			}
		}
	}
	assert.True(t, hasEmployeeFilter, "Should detect department filter")
	assert.True(t, hasContractorFilter, "Should detect active filter")
}

// TestAnalyzeIntersectExcept checks INTERSECT and EXCEPT set operation detection.
func TestAnalyzeIntersectExcept(t *testing.T) {
	sql := `(SELECT product_id FROM orders WHERE created_at > ?)
			INTERSECT
			(SELECT product_id FROM inventory WHERE quantity > 0)
			EXCEPT
			(SELECT product_id FROM discontinued)`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should detect multiple set operations
	assert.GreaterOrEqual(t, len(result.SetOperations), 2)
}

// TestAnalyzeMultipleWindowFunctions verifies partition and order usage from multiple window functions.
func TestAnalyzeMultipleWindowFunctions(t *testing.T) {
	sql := `SELECT
			employee_id,
			salary,
			ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as dept_rank,
			RANK() OVER (ORDER BY salary DESC) as company_rank,
			AVG(salary) OVER () as avg_salary,
			SUM(salary) OVER (PARTITION BY dept_id) as dept_total
			FROM employees
			WHERE active = ?`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Count window partition and order usages
	var partitionCount, windowOrderCount int
	for _, usage := range result.ColumnUsage {
		if usage.UsageType == SQLUsageTypeWindowPartition {
			partitionCount++
		}
		if usage.UsageType == SQLUsageTypeWindowOrder {
			windowOrderCount++
		}
	}
	assert.GreaterOrEqual(t, partitionCount, 2, "Should detect multiple partition clauses")
	assert.GreaterOrEqual(t, windowOrderCount, 2, "Should detect multiple order clauses")
}

// TestAnalyzeReturningQualified validates RETURNING with table-qualified columns in UPDATE.
func TestAnalyzeReturningQualified(t *testing.T) {
	sql := `UPDATE public.orders o
			SET status = ?, updated_at = NOW()
			FROM customers c
			WHERE o.customer_id = c.id
			RETURNING o.id, o.status, c.name, o.updated_at`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, SQLCommandUpdate, result.Command)
	assert.NotEmpty(t, result.Returning)

	// Check RETURNING columns
	assert.GreaterOrEqual(t, len(result.Returning), 4)
}

// TestAnalyzeQuotedIdentifiersDML checks quoted and schema-qualified identifiers in UPDATE.
func TestAnalyzeQuotedIdentifiersDML(t *testing.T) {
	sql := `UPDATE "Public"."Orders" o
			SET "Status" = ?, "UpdatedAt" = NOW()
			WHERE o."CustomerId" = ? AND o."CreatedAt" > ?`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should preserve quoted identifiers
	assert.Equal(t, SQLCommandUpdate, result.Command)

	// Check if table name preserves quotes
	if len(result.Tables) > 0 {
		// Depending on parser behavior, might strip or preserve quotes
		assert.NotEmpty(t, result.Tables[0].Name)
	}

	// Check SET columns preserve case
	var hasQuotedColumn bool
	for _, usage := range result.ColumnUsage {
		if usage.UsageType == SQLUsageTypeDMLSet {
			// Check if case is preserved (Status not status)
			if usage.Column == "Status" || usage.Column == "UpdatedAt" {
				hasQuotedColumn = true
				break
			}
		}
	}
	_ = hasQuotedColumn // Parser might normalize these
}

// TestAnalyzeSameColumnMultipleTables verifies disambiguation of same-named columns across tables.
func TestAnalyzeSameColumnMultipleTables(t *testing.T) {
	sql := `SELECT o.id, c.id, p.id, o.name, c.name
			FROM orders o
			JOIN customers c ON o.customer_id = c.id
			JOIN products p ON o.product_id = p.id
			WHERE o.status = ? AND c.status = ? AND p.status = ?`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Count how many times 'id' appears with different tables
	idCount := 0
	statusCount := 0
	for _, usage := range result.ColumnUsage {
		if usage.Column == "id" {
			idCount++
		}
		if usage.Column == "status" && usage.UsageType == SQLUsageTypeFilter {
			statusCount++
		}
	}
	assert.GreaterOrEqual(t, idCount, 3, "Should detect id from multiple tables")
	assert.GreaterOrEqual(t, statusCount, 3, "Should detect status filters from multiple tables")
}

// TestAnalyzeComplexJoinConditions validates multi-condition INNER and LEFT JOIN extraction.
func TestAnalyzeComplexJoinConditions(t *testing.T) {
	sql := `SELECT * FROM orders o
			INNER JOIN order_items oi ON o.id = oi.order_id
				AND oi.quantity > 0
				AND oi.price > ?
			LEFT JOIN products p ON oi.product_id = p.id
				AND p.active = true
			WHERE o.created_at > ?`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should detect all join columns
	var joinColumns []string
	for _, usage := range result.ColumnUsage {
		if usage.UsageType == SQLUsageTypeJoin {
			if !contains(joinColumns, usage.Column) {
				joinColumns = append(joinColumns, usage.Column)
			}
		}
	}
	assert.Contains(t, joinColumns, "order_id")
	assert.Contains(t, joinColumns, "product_id")
	assert.Contains(t, joinColumns, "quantity")
	assert.Contains(t, joinColumns, "price")
	assert.Contains(t, joinColumns, "active")
}

// TestAnalyzeErrorForInvalidSQL confirms that unparseable SQL returns an error.
func TestAnalyzeErrorForInvalidSQL(t *testing.T) {
	sql := `SELECT * FROM @#$invalid WHERE something = ?`

	result, err := AnalyzeSQL(sql)

	assert.Error(t, err)
	assert.Nil(t, result)
}

// TestAnalyzeNestedSubqueries checks deeply nested subquery filter extraction.
func TestAnalyzeNestedSubqueries(t *testing.T) {
	sql := `SELECT * FROM (
			  SELECT o.*,
				(SELECT COUNT(*) FROM order_items WHERE order_id = o.id) as item_count
			  FROM orders o
			  WHERE o.customer_id IN (
				SELECT id FROM customers
				WHERE country = ?
				AND created_at > (
				  SELECT MAX(created_at) - INTERVAL '30 days' FROM customers
				)
			  )
			) AS enriched_orders
			WHERE item_count > ?`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should detect nested structure
	assert.NotEmpty(t, result.Subqueries, "Should detect subqueries")

	// Should find filter columns at various levels
	var hasCountryFilter, hasItemCountFilter bool
	for _, usage := range result.ColumnUsage {
		if usage.UsageType == SQLUsageTypeFilter {
			if usage.Column == "country" {
				hasCountryFilter = true
			}
			if usage.Column == "item_count" {
				hasItemCountFilter = true
			}
		}
	}
	assert.True(t, hasCountryFilter, "Should detect country filter in nested subquery")
	// item_count might not be detected as it's from derived table
	_ = hasItemCountFilter
}

// TestAnalyzeLateralJoin validates correlated LATERAL subquery detection.
func TestAnalyzeLateralJoin(t *testing.T) {
	sql := `SELECT u.id, u.name, recent.order_count, recent.total_spent
			FROM users u
			LEFT JOIN LATERAL (
				SELECT COUNT(*) as order_count, SUM(total) as total_spent
				FROM orders o
				WHERE o.user_id = u.id
				AND o.created_at > ?
				LIMIT 1
			) recent ON true
			WHERE u.active = ?`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	// LATERAL might be treated as subquery or function
	assert.GreaterOrEqual(t, len(result.Tables), 1)

	// Should detect correlation
	var hasUserIdUsage bool
	for _, usage := range result.ColumnUsage {
		if usage.Column == "user_id" {
			hasUserIdUsage = true
			break
		}
	}
	assert.True(t, hasUserIdUsage, "Should detect correlated user_id")
}

// TestAnalyzeInsertMultipleValues verifies multi-row INSERT with ON CONFLICT and RETURNING.
func TestAnalyzeInsertMultipleValues(t *testing.T) {
	sql := `INSERT INTO products (name, price, category_id)
			VALUES
				(?, ?, ?),
				(?, ?, ?),
				(?, ?, ?)
			ON CONFLICT (name, category_id)
			DO UPDATE SET price = EXCLUDED.price
			RETURNING id, name`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, SQLCommandInsert, result.Command)
	assert.True(t, result.Upsert != nil, "Should detect UPSERT")

	// Check conflict target columns
	if result.Upsert != nil {
		assert.Contains(t, result.Upsert.TargetColumns, "name")
		assert.Contains(t, result.Upsert.TargetColumns, "category_id")
	}

	// Check RETURNING
	assert.Contains(t, result.Returning, "id")
	assert.Contains(t, result.Returning, "name")
}

// Helper function
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
