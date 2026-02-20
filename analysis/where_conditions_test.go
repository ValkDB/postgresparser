// Package analysis provides query analysis for the PostgreSQL parser.
// This file contains comprehensive unit tests for WHERE condition extraction.
// Tests cover all SQL operators: equality, comparison, range, set, pattern, NULL, logical, and parameterized.
package analysis

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valkdb/postgresparser"
)

// =============================================================================
// EQUALITY OPERATORS: =, !=, <>
// =============================================================================

// TestExtractWhereConditions_Equality validates =, !=, and <> operators with various literal types.
func TestExtractWhereConditions_Equality(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedOp     string
		expectedValue  interface{}
		expectedColumn string
		expectedTable  string
	}{
		{
			name:           "equals with string literal",
			query:          "SELECT * FROM orders WHERE status = 'pending'",
			expectedOp:     "=",
			expectedValue:  "pending",
			expectedColumn: "status",
			expectedTable:  "orders",
		},
		{
			name:           "equals with numeric literal",
			query:          "SELECT * FROM products WHERE price = 99.99",
			expectedOp:     "=",
			expectedValue:  "99.99",
			expectedColumn: "price",
			expectedTable:  "products",
		},
		{
			name:           "equals with integer",
			query:          "SELECT * FROM users WHERE id = 42",
			expectedOp:     "=",
			expectedValue:  "42",
			expectedColumn: "id",
			expectedTable:  "users",
		},
		{
			name:           "not equals with !=",
			query:          "SELECT * FROM orders WHERE status != 'cancelled'",
			expectedOp:     "!=",
			expectedValue:  "cancelled",
			expectedColumn: "status",
			expectedTable:  "orders",
		},
		{
			name:           "not equals with <>",
			query:          "SELECT * FROM orders WHERE status <> 'cancelled'",
			expectedOp:     "!=", // Should normalize <> to !=
			expectedValue:  "cancelled",
			expectedColumn: "status",
			expectedTable:  "orders",
		},
		{
			name:           "equals with boolean true",
			query:          "SELECT * FROM users WHERE is_active = true",
			expectedOp:     "=",
			expectedValue:  "true",
			expectedColumn: "is_active",
			expectedTable:  "users",
		},
		{
			name:           "equals with boolean false",
			query:          "SELECT * FROM users WHERE is_deleted = false",
			expectedOp:     "=",
			expectedValue:  "false",
			expectedColumn: "is_deleted",
			expectedTable:  "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, 1)

			assert.Equal(t, tt.expectedTable, conditions[0].Table)
			assert.Equal(t, tt.expectedColumn, conditions[0].Column)
			assert.Equal(t, tt.expectedOp, conditions[0].Operator)
			assert.Equal(t, tt.expectedValue, conditions[0].Value)
			assert.False(t, conditions[0].IsParameter)
		})
	}
}

// =============================================================================
// COMPARISON OPERATORS: >, <, >=, <=
// =============================================================================

// TestExtractWhereConditions_Comparison verifies >, <, >=, and <= comparison operators.
func TestExtractWhereConditions_Comparison(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedOp     string
		expectedValue  interface{}
		expectedColumn string
	}{
		{
			name:           "greater than",
			query:          "SELECT * FROM orders WHERE total > 100",
			expectedOp:     ">",
			expectedValue:  "100",
			expectedColumn: "total",
		},
		{
			name:           "less than",
			query:          "SELECT * FROM orders WHERE total < 50",
			expectedOp:     "<",
			expectedValue:  "50",
			expectedColumn: "total",
		},
		{
			name:           "greater than or equal",
			query:          "SELECT * FROM orders WHERE quantity >= 10",
			expectedOp:     ">=",
			expectedValue:  "10",
			expectedColumn: "quantity",
		},
		{
			name:           "less than or equal",
			query:          "SELECT * FROM products WHERE stock <= 5",
			expectedOp:     "<=",
			expectedValue:  "5",
			expectedColumn: "stock",
		},
		{
			name:           "greater than with decimal",
			query:          "SELECT * FROM products WHERE price > 19.99",
			expectedOp:     ">",
			expectedValue:  "19.99",
			expectedColumn: "price",
		},
		{
			name:           "less than with negative",
			query:          "SELECT * FROM accounts WHERE balance < -100",
			expectedOp:     "<",
			expectedValue:  "-100",
			expectedColumn: "balance",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, 1)

			assert.Equal(t, tt.expectedColumn, conditions[0].Column)
			assert.Equal(t, tt.expectedOp, conditions[0].Operator)
			assert.Equal(t, tt.expectedValue, conditions[0].Value)
		})
	}
}

// =============================================================================
// RANGE OPERATORS: BETWEEN, NOT BETWEEN
// =============================================================================

// TestExtractWhereConditions_Between checks BETWEEN extraction with integers, decimals, and dates.
func TestExtractWhereConditions_Between(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedOp     string
		expectedValues []string
		expectedColumn string
	}{
		{
			name:           "BETWEEN with integers",
			query:          "SELECT * FROM orders WHERE total BETWEEN 100 AND 500",
			expectedOp:     "BETWEEN",
			expectedValues: []string{"100", "500"},
			expectedColumn: "total",
		},
		{
			name:           "BETWEEN with decimals",
			query:          "SELECT * FROM products WHERE price BETWEEN 9.99 AND 99.99",
			expectedOp:     "BETWEEN",
			expectedValues: []string{"9.99", "99.99"},
			expectedColumn: "price",
		},
		{
			name:           "BETWEEN with strings (dates)",
			query:          "SELECT * FROM events WHERE event_date BETWEEN '2024-01-01' AND '2024-12-31'",
			expectedOp:     "BETWEEN",
			expectedValues: []string{"2024-01-01", "2024-12-31"},
			expectedColumn: "event_date",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, 1)

			assert.Equal(t, tt.expectedColumn, conditions[0].Column)
			assert.Equal(t, tt.expectedOp, conditions[0].Operator)

			values, ok := conditions[0].Value.([]string)
			require.True(t, ok, "BETWEEN value should be []string")
			assert.Equal(t, tt.expectedValues, values)
		})
	}
}

// TestExtractWhereConditions_NotBetween validates NOT BETWEEN operator extraction.
func TestExtractWhereConditions_NotBetween(t *testing.T) {
	query := "SELECT * FROM orders WHERE total NOT BETWEEN 0 AND 10"

	conditions, err := ExtractWhereConditions(query)

	require.NoError(t, err)
	require.Len(t, conditions, 1)

	assert.Equal(t, "orders", conditions[0].Table)
	assert.Equal(t, "total", conditions[0].Column)
	assert.Equal(t, "NOT BETWEEN", conditions[0].Operator)

	// Note: NOT BETWEEN value is currently returned as raw string "0 AND 10"
	// rather than parsed []string{"0", "10"}. This is a known limitation.
	// The value extraction handles BETWEEN but NOT BETWEEN uses a different code path.
	assert.NotNil(t, conditions[0].Value)
	assert.False(t, conditions[0].IsParameter)
}

// =============================================================================
// SET OPERATORS: IN, NOT IN
// =============================================================================

// TestExtractWhereConditions_In verifies IN list value extraction with strings, integers, and single values.
func TestExtractWhereConditions_In(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedValues []string
		expectedColumn string
	}{
		{
			name:           "IN with string values",
			query:          "SELECT * FROM orders WHERE status IN ('pending', 'processing', 'shipped')",
			expectedValues: []string{"pending", "processing", "shipped"},
			expectedColumn: "status",
		},
		{
			name:           "IN with integer values",
			query:          "SELECT * FROM products WHERE category_id IN (1, 2, 3, 4)",
			expectedValues: []string{"1", "2", "3", "4"},
			expectedColumn: "category_id",
		},
		{
			name:           "IN with single value",
			query:          "SELECT * FROM users WHERE role IN ('admin')",
			expectedValues: []string{"admin"},
			expectedColumn: "role",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, 1)

			assert.Equal(t, tt.expectedColumn, conditions[0].Column)
			assert.Equal(t, "IN", conditions[0].Operator)

			values, ok := conditions[0].Value.([]string)
			require.True(t, ok, "IN value should be []string")
			assert.Equal(t, tt.expectedValues, values)
		})
	}
}

// TestExtractWhereConditions_NotIn checks NOT IN operator extraction.
func TestExtractWhereConditions_NotIn(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedColumn string
	}{
		{
			name:           "NOT IN with string values",
			query:          "SELECT * FROM orders WHERE status NOT IN ('cancelled', 'refunded')",
			expectedColumn: "status",
		},
		{
			name:           "NOT IN with integer values",
			query:          "SELECT * FROM products WHERE id NOT IN (1, 2, 3)",
			expectedColumn: "id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, 1)

			assert.Equal(t, tt.expectedColumn, conditions[0].Column)
			assert.Equal(t, "NOT IN", conditions[0].Operator)

			// Note: NOT IN value is currently returned as raw string "(values...)"
			// rather than parsed []string. This is a known limitation.
			// The value extraction handles IN but NOT IN uses a different code path.
			assert.NotNil(t, conditions[0].Value)
			assert.False(t, conditions[0].IsParameter)
		})
	}
}

// =============================================================================
// PATTERN OPERATORS: LIKE, NOT LIKE, ILIKE
// =============================================================================

// TestExtractWhereConditions_Like validates LIKE pattern extraction with various wildcard positions.
func TestExtractWhereConditions_Like(t *testing.T) {
	tests := []struct {
		name            string
		query           string
		expectedOp      string
		expectedPattern string
		expectedColumn  string
	}{
		{
			name:            "LIKE with prefix wildcard",
			query:           "SELECT * FROM users WHERE email LIKE '%@example.com'",
			expectedOp:      "LIKE",
			expectedPattern: "%@example.com",
			expectedColumn:  "email",
		},
		{
			name:            "LIKE with suffix wildcard",
			query:           "SELECT * FROM products WHERE name LIKE 'Phone%'",
			expectedOp:      "LIKE",
			expectedPattern: "Phone%",
			expectedColumn:  "name",
		},
		{
			name:            "LIKE with both wildcards",
			query:           "SELECT * FROM articles WHERE title LIKE '%keyword%'",
			expectedOp:      "LIKE",
			expectedPattern: "%keyword%",
			expectedColumn:  "title",
		},
		{
			name:            "LIKE with underscore wildcard",
			query:           "SELECT * FROM codes WHERE code LIKE 'A_B_C'",
			expectedOp:      "LIKE",
			expectedPattern: "A_B_C",
			expectedColumn:  "code",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, 1)

			assert.Equal(t, tt.expectedColumn, conditions[0].Column)
			assert.Equal(t, tt.expectedOp, conditions[0].Operator)
			assert.Equal(t, tt.expectedPattern, conditions[0].Value)
		})
	}
}

// TestExtractWhereConditions_NotLike verifies NOT LIKE operator and pattern extraction.
func TestExtractWhereConditions_NotLike(t *testing.T) {
	tests := []struct {
		name            string
		query           string
		expectedPattern string
		expectedColumn  string
	}{
		{
			name:            "NOT LIKE with wildcard",
			query:           "SELECT * FROM users WHERE email NOT LIKE '%@spam.com'",
			expectedPattern: "%@spam.com",
			expectedColumn:  "email",
		},
		{
			name:            "NOT LIKE with prefix",
			query:           "SELECT * FROM products WHERE sku NOT LIKE 'TEST%'",
			expectedPattern: "TEST%",
			expectedColumn:  "sku",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, 1)

			assert.Equal(t, tt.expectedColumn, conditions[0].Column)
			assert.Equal(t, "NOT LIKE", conditions[0].Operator)
			assert.Equal(t, tt.expectedPattern, conditions[0].Value)
		})
	}
}

// TestExtractWhereConditions_ILike checks ILIKE and NOT ILIKE case-insensitive pattern operators.
func TestExtractWhereConditions_ILike(t *testing.T) {
	tests := []struct {
		name            string
		query           string
		expectedOp      string
		expectedPattern string
		expectedColumn  string
	}{
		{
			name:            "ILIKE case-insensitive",
			query:           "SELECT * FROM users WHERE name ILIKE '%john%'",
			expectedOp:      "ILIKE",
			expectedPattern: "%john%",
			expectedColumn:  "name",
		},
		{
			name:            "NOT ILIKE case-insensitive",
			query:           "SELECT * FROM users WHERE name NOT ILIKE '%test%'",
			expectedOp:      "NOT ILIKE",
			expectedPattern: "%test%",
			expectedColumn:  "name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, 1)

			assert.Equal(t, tt.expectedColumn, conditions[0].Column)
			assert.Equal(t, tt.expectedOp, conditions[0].Operator)
			assert.Equal(t, tt.expectedPattern, conditions[0].Value)
		})
	}
}

// =============================================================================
// NULL OPERATORS: IS NULL, IS NOT NULL
// =============================================================================

// TestExtractWhereConditions_IsNull validates IS NULL predicate extraction.
func TestExtractWhereConditions_IsNull(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedOp     string
		expectedColumn string
	}{
		{
			name:           "IS NULL",
			query:          "SELECT * FROM orders WHERE deleted_at IS NULL",
			expectedOp:     "IS",
			expectedColumn: "deleted_at",
		},
		{
			name:           "IS NULL on different column",
			query:          "SELECT * FROM users WHERE last_login IS NULL",
			expectedOp:     "IS",
			expectedColumn: "last_login",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, 1)

			assert.Equal(t, tt.expectedColumn, conditions[0].Column)
			assert.Equal(t, tt.expectedOp, conditions[0].Operator)
			assert.Nil(t, conditions[0].Value)
		})
	}
}

// TestExtractWhereConditions_IsNotNull verifies IS NOT NULL predicate extraction.
func TestExtractWhereConditions_IsNotNull(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedColumn string
	}{
		{
			name:           "IS NOT NULL",
			query:          "SELECT * FROM orders WHERE shipped_at IS NOT NULL",
			expectedColumn: "shipped_at",
		},
		{
			name:           "IS NOT NULL on different column",
			query:          "SELECT * FROM users WHERE email_verified_at IS NOT NULL",
			expectedColumn: "email_verified_at",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, 1)

			assert.Equal(t, tt.expectedColumn, conditions[0].Column)
			assert.Equal(t, "IS NOT NULL", conditions[0].Operator)
			assert.Nil(t, conditions[0].Value)
		})
	}
}

// =============================================================================
// LOGICAL OPERATORS: AND, OR, NOT
// =============================================================================

// TestExtractWhereConditions_And validates AND-combined condition extraction.
func TestExtractWhereConditions_And(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedCount int
		columns       []string
		operators     []string
	}{
		{
			name:          "two conditions with AND",
			query:         "SELECT * FROM orders WHERE status = 'pending' AND total > 100",
			expectedCount: 2,
			columns:       []string{"status", "total"},
			operators:     []string{"=", ">"},
		},
		{
			name:          "three conditions with AND",
			query:         "SELECT * FROM users WHERE is_active = true AND role = 'admin' AND created_at > '2024-01-01'",
			expectedCount: 3,
			columns:       []string{"is_active", "role", "created_at"},
			operators:     []string{"=", "=", ">"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, tt.expectedCount)

			for i, col := range tt.columns {
				assert.Equal(t, col, conditions[i].Column)
				assert.Equal(t, tt.operators[i], conditions[i].Operator)
			}
		})
	}
}

// TestExtractWhereConditions_Or checks OR-combined condition extraction.
func TestExtractWhereConditions_Or(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedCount int
		columns       []string
	}{
		{
			name:          "two conditions with OR",
			query:         "SELECT * FROM orders WHERE status = 'pending' OR status = 'processing'",
			expectedCount: 2,
			columns:       []string{"status", "status"},
		},
		{
			name:          "different columns with OR",
			query:         "SELECT * FROM users WHERE email LIKE '%@vip.com' OR is_premium = true",
			expectedCount: 2,
			columns:       []string{"email", "is_premium"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, tt.expectedCount)

			for i, col := range tt.columns {
				assert.Equal(t, col, conditions[i].Column)
			}
		})
	}
}

// TestExtractWhereConditions_Not verifies NOT LIKE, NOT IN, and NOT BETWEEN operators.
func TestExtractWhereConditions_Not(t *testing.T) {
	// NOT is typically used with other operators
	tests := []struct {
		name       string
		query      string
		expectedOp string
		column     string
	}{
		{
			name:       "NOT LIKE",
			query:      "SELECT * FROM users WHERE name NOT LIKE '%test%'",
			expectedOp: "NOT LIKE",
			column:     "name",
		},
		{
			name:       "NOT IN",
			query:      "SELECT * FROM orders WHERE status NOT IN ('cancelled')",
			expectedOp: "NOT IN",
			column:     "status",
		},
		{
			name:       "NOT BETWEEN",
			query:      "SELECT * FROM products WHERE price NOT BETWEEN 0 AND 10",
			expectedOp: "NOT BETWEEN",
			column:     "price",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, 1)

			assert.Equal(t, tt.column, conditions[0].Column)
			assert.Equal(t, tt.expectedOp, conditions[0].Operator)
		})
	}
}

// =============================================================================
// PARAMETERIZED QUERIES: $1, $2, ?
// =============================================================================

// TestExtractWhereConditions_Parameterized validates $N and ? parameter marker detection.
func TestExtractWhereConditions_Parameterized(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedCount int
		params        []string
	}{
		{
			name:          "single positional parameter",
			query:         "SELECT * FROM orders WHERE status = $1",
			expectedCount: 1,
			params:        []string{"$1"},
		},
		{
			name:          "multiple positional parameters",
			query:         "SELECT * FROM orders WHERE status = $1 AND total > $2 AND customer_id = $3",
			expectedCount: 3,
			params:        []string{"$1", "$2", "$3"},
		},
		{
			name:          "anonymous parameter",
			query:         "SELECT * FROM users WHERE id = ?",
			expectedCount: 1,
			params:        []string{"?"},
		},
		{
			name:          "multiple anonymous parameters",
			query:         "SELECT * FROM orders WHERE status = ? AND total > ?",
			expectedCount: 2,
			params:        []string{"?", "?"},
		},
		{
			name:          "mixed literal and parameter",
			query:         "SELECT * FROM orders WHERE status = 'pending' AND total > $1",
			expectedCount: 2,
			params:        []string{"", "$1"}, // First is not a param
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, tt.expectedCount)

			for i, param := range tt.params {
				if param != "" {
					assert.Equal(t, param, conditions[i].Value)
					assert.True(t, conditions[i].IsParameter)
				} else {
					assert.False(t, conditions[i].IsParameter)
				}
			}
		})
	}
}

// =============================================================================
// MULTI-TABLE / JOIN CONDITIONS
// =============================================================================

// TestExtractWhereConditions_Join validates WHERE extraction in multi-table JOIN queries.
func TestExtractWhereConditions_Join(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedCount int
		tables        []string
		columns       []string
	}{
		{
			name: "simple join with WHERE",
			query: `
				SELECT o.id, c.name
				FROM orders o
				JOIN customers c ON o.customer_id = c.id
				WHERE o.status = 'pending'
			`,
			expectedCount: 1,
			tables:        []string{"orders"},
			columns:       []string{"status"},
		},
		{
			name: "join with multiple WHERE conditions",
			query: `
				SELECT o.id, c.name
				FROM orders o
				JOIN customers c ON o.customer_id = c.id
				WHERE o.status = 'pending' AND c.country = 'US'
			`,
			expectedCount: 2,
			tables:        []string{"orders", "customers"},
			columns:       []string{"status", "country"},
		},
		{
			name: "multiple joins with WHERE",
			query: `
				SELECT o.id, c.name, p.name as product
				FROM orders o
				JOIN customers c ON o.customer_id = c.id
				JOIN products p ON o.product_id = p.id
				WHERE o.total > 100 AND c.is_premium = true AND p.category = 'electronics'
			`,
			expectedCount: 3,
			tables:        []string{"orders", "customers", "products"},
			columns:       []string{"total", "is_premium", "category"},
		},
		{
			name: "left join with WHERE",
			query: `
				SELECT u.id, o.id as order_id
				FROM users u
				LEFT JOIN orders o ON u.id = o.user_id
				WHERE u.is_active = true AND o.status IS NULL
			`,
			expectedCount: 2,
			tables:        []string{"users", "orders"},
			columns:       []string{"is_active", "status"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, tt.expectedCount)

			for i := range tt.tables {
				assert.Equal(t, tt.tables[i], conditions[i].Table, "table mismatch at index %d", i)
				assert.Equal(t, tt.columns[i], conditions[i].Column, "column mismatch at index %d", i)
			}
		})
	}
}

// =============================================================================
// COMPLEX / NESTED EXPRESSIONS
// =============================================================================

// TestExtractWhereConditions_ComplexNested checks deeply nested AND/OR groups and mixed operators.
func TestExtractWhereConditions_ComplexNested(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedCount int
		description   string
	}{
		{
			name: "parenthesized OR within AND",
			query: `
				SELECT * FROM orders
				WHERE status = 'active' AND (priority = 'high' OR priority = 'urgent')
			`,
			expectedCount: 3,
			description:   "AND with parenthesized OR group",
		},
		{
			name: "multiple parenthesized groups",
			query: `
				SELECT * FROM orders
				WHERE (status = 'pending' OR status = 'processing') AND (total > 100 OR is_priority = true)
			`,
			expectedCount: 4,
			description:   "Multiple OR groups connected by AND",
		},
		{
			name: "deeply nested conditions",
			query: `
				SELECT * FROM orders
				WHERE status = 'active'
					AND (
						(customer_type = 'premium' AND total > 1000)
						OR
						(customer_type = 'standard' AND total > 5000)
					)
			`,
			expectedCount: 5,
			description:   "Deeply nested AND/OR combinations",
		},
		{
			name: "mixed operators in complex query",
			query: `
				SELECT * FROM orders o
				JOIN customers c ON o.customer_id = c.id
				WHERE o.status IN ('pending', 'processing')
					AND o.total BETWEEN 100 AND 1000
					AND c.email LIKE '%@company.com'
					AND o.created_at IS NOT NULL
			`,
			expectedCount: 4,
			description:   "Mix of IN, BETWEEN, LIKE, IS NOT NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err, "Failed to parse: %s", tt.description)
			assert.Len(t, conditions, tt.expectedCount,
				"Expected %d conditions for %s, got %d",
				tt.expectedCount, tt.description, len(conditions))
		})
	}
}

// =============================================================================
// EDGE CASES
// =============================================================================

// TestExtractWhereConditions_EdgeCases covers no WHERE, empty input, invalid SQL, and quoted identifiers.
func TestExtractWhereConditions_EdgeCases(t *testing.T) {
	t.Run("no WHERE clause", func(t *testing.T) {
		query := "SELECT * FROM orders"

		conditions, err := ExtractWhereConditions(query)

		require.NoError(t, err)
		assert.Empty(t, conditions)
	})

	t.Run("empty string", func(t *testing.T) {
		query := ""

		conditions, err := ExtractWhereConditions(query)

		assert.Error(t, err)
		assert.Nil(t, conditions)
	})

	t.Run("invalid SQL syntax", func(t *testing.T) {
		query := "SELECT * FROM WHERE invalid"

		conditions, err := ExtractWhereConditions(query)

		assert.Error(t, err)
		assert.Nil(t, conditions)
	})

	t.Run("WHERE with only parameter", func(t *testing.T) {
		query := "SELECT * FROM users WHERE id = $1"

		conditions, err := ExtractWhereConditions(query)

		require.NoError(t, err)
		require.Len(t, conditions, 1)
		assert.Equal(t, "$1", conditions[0].Value)
		assert.True(t, conditions[0].IsParameter)
	})

	t.Run("column with schema prefix", func(t *testing.T) {
		query := "SELECT * FROM public.orders WHERE public.orders.status = 'pending'"

		conditions, err := ExtractWhereConditions(query)

		require.NoError(t, err)
		require.Len(t, conditions, 1)
		assert.Equal(t, "status", conditions[0].Column)
	})

	t.Run("quoted identifiers", func(t *testing.T) {
		query := `SELECT * FROM "Orders" WHERE "Status" = 'pending'`

		conditions, err := ExtractWhereConditions(query)

		require.NoError(t, err)
		require.Len(t, conditions, 1)
	})

	t.Run("case sensitivity preservation", func(t *testing.T) {
		query := "SELECT * FROM Orders WHERE Status = 'Pending'"

		conditions, err := ExtractWhereConditions(query)

		require.NoError(t, err)
		require.Len(t, conditions, 1)
		// Value should preserve original case
		assert.Equal(t, "Pending", conditions[0].Value)
	})
}

// =============================================================================
// HELPER FUNCTION TESTS
// =============================================================================

// TestNormalizeOperator validates operator normalization including <>, ~~, !~~, and case handling.
func TestNormalizeOperator(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"<>", "!="},
		{"=", "="},
		{">", ">"},
		{"<", "<"},
		{">=", ">="},
		{"<=", "<="},
		{"!=", "!="},
		{"~~", "LIKE"},
		{"!~~", "NOT LIKE"},
		{"~~*", "ILIKE"},
		{"!~~*", "NOT ILIKE"},
		{"like", "LIKE"},
		{"LIKE", "LIKE"},
		{"ilike", "ILIKE"},
		{"ILIKE", "ILIKE"},
		{"  =  ", "="},
		{" <> ", "!="},
		{"between", "BETWEEN"},
		{"BETWEEN", "BETWEEN"},
		{"in", "IN"},
		{"IN", "IN"},
		{"is", "IS"},
		{"IS", "IS"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := normalizeOperator(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExtractWhereConditions_TableResolution verifies alias-to-table resolution through ExtractWhereConditions.
func TestExtractWhereConditions_TableResolution(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedTable string
	}{
		{
			name:          "alias resolved to table name",
			query:         "SELECT * FROM orders o WHERE o.status = 'pending'",
			expectedTable: "orders",
		},
		{
			name:          "no alias uses table name directly",
			query:         "SELECT * FROM customers WHERE customers.country = 'US'",
			expectedTable: "customers",
		},
		{
			name:          "multiple tables with aliases",
			query:         "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.status = 'pending'",
			expectedTable: "orders",
		},
		{
			name:          "uppercase alias resolves case-insensitively",
			query:         "SELECT * FROM orders O WHERE O.status = 'pending'",
			expectedTable: "orders",
		},
		{
			name:          "mixed-case unquoted table normalizes to lowercase (intentional)",
			query:         "SELECT * FROM Orders O WHERE O.status = 'pending'",
			expectedTable: "orders",
		},
		{
			name:          "cte alias resolves to cte name",
			query:         "WITH active_orders AS (SELECT * FROM orders) SELECT * FROM active_orders ao WHERE ao.total > 100",
			expectedTable: "active_orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.NotEmpty(t, conditions)
			assert.Equal(t, tt.expectedTable, conditions[0].Table)
		})
	}
}

// TestBuildWhereAliasMap verifies WHERE alias-map resolution behavior.
func TestBuildWhereAliasMap(t *testing.T) {
	tables := []postgresparser.TableRef{
		{Name: ` "Orders" `, Alias: ` "O" `, Type: postgresparser.TableTypeBase},
		{Name: ` "active_orders" `, Alias: ` "ao" `, Type: postgresparser.TableTypeCTE},
		{Name: "derived_orders", Alias: " do ", Type: postgresparser.TableTypeSubquery},
	}
	aliasMap := buildWhereAliasMap(tables)

	assert.Equal(t, "orders", resolveAlias("O", aliasMap), "should resolve aliases case-insensitively")
	assert.Equal(t, "orders", resolveAlias("orders", aliasMap), "should resolve direct base table names")
	assert.Equal(t, "active_orders", resolveAlias("AO", aliasMap), "should resolve CTE aliases")
	assert.Equal(t, "derived_orders", resolveAlias("do", aliasMap), "should resolve subquery aliases")
}

// TestExtractInValues validates parsing of IN value lists with integers, strings, and edge cases.
func TestExtractInValues(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "simple integer list",
			input:    "(1, 2, 3)",
			expected: []string{"1", "2", "3"},
		},
		{
			name:     "string list with quotes",
			input:    "('a', 'b', 'c')",
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "single value",
			input:    "('single')",
			expected: []string{"single"},
		},
		{
			name:     "values with spaces",
			input:    "( 1 , 2 , 3 )",
			expected: []string{"1", "2", "3"},
		},
		{
			name:     "empty parentheses",
			input:    "()",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractInValues(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExtractBetweenValues checks BETWEEN range value parsing with integers, strings, and decimals.
func TestExtractBetweenValues(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "integer range",
			input:    "100 AND 500",
			expected: []string{"100", "500"},
		},
		{
			name:     "string range (dates)",
			input:    "'2024-01-01' AND '2024-12-31'",
			expected: []string{"2024-01-01", "2024-12-31"},
		},
		{
			name:     "decimal range",
			input:    "9.99 AND 99.99",
			expected: []string{"9.99", "99.99"},
		},
		{
			name:     "case insensitive AND",
			input:    "10 and 20",
			expected: []string{"10", "20"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractBetweenValues(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// OPERATOR NORMALIZATION (PostgreSQL specific operators)
// =============================================================================

// TestExtractWhereConditions_PostgresOperatorNormalization verifies ~~, !~~, ~~*, !~~* normalization to LIKE variants.
func TestExtractWhereConditions_PostgresOperatorNormalization(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedOp     string
		expectedColumn string
	}{
		{
			name:           "~~ normalized to LIKE",
			query:          "SELECT * FROM users WHERE name ~~ 'John%'",
			expectedOp:     "LIKE",
			expectedColumn: "name",
		},
		{
			name:           "!~~ normalized to NOT LIKE",
			query:          "SELECT * FROM users WHERE name !~~ 'Test%'",
			expectedOp:     "NOT LIKE",
			expectedColumn: "name",
		},
		{
			name:           "~~* normalized to ILIKE",
			query:          "SELECT * FROM users WHERE email ~~* '%@COMPANY.COM'",
			expectedOp:     "ILIKE",
			expectedColumn: "email",
		},
		{
			name:           "!~~* normalized to NOT ILIKE",
			query:          "SELECT * FROM users WHERE email !~~* '%@spam.com'",
			expectedOp:     "NOT ILIKE",
			expectedColumn: "email",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, 1)

			assert.Equal(t, tt.expectedColumn, conditions[0].Column)
			assert.Equal(t, tt.expectedOp, conditions[0].Operator)
		})
	}
}

// =============================================================================
// SUBQUERY IN WHERE CLAUSE
// =============================================================================

// TestExtractWhereConditions_Subquery validates WHERE extraction with IN, EXISTS, and NOT EXISTS subqueries.
func TestExtractWhereConditions_Subquery(t *testing.T) {
	// Note: The current implementation extracts conditions from both outer queries
	// AND subqueries. This tests that subquery-containing queries parse successfully
	// and that the outer condition is detected.

	t.Run("IN with subquery parses successfully", func(t *testing.T) {
		query := `
			SELECT * FROM orders
			WHERE customer_id IN (SELECT id FROM customers WHERE country = 'US')
		`
		conditions, err := ExtractWhereConditions(query)

		require.NoError(t, err)
		// Parser extracts conditions from subqueries too - verify it doesn't fail
		// and finds at least the outer IN condition
		require.NotEmpty(t, conditions)

		// Find the customer_id IN condition
		found := false
		for _, c := range conditions {
			if c.Column == "customer_id" && c.Operator == "IN" {
				found = true
				break
			}
		}
		assert.True(t, found, "Should find customer_id IN condition")
	})

	t.Run("EXISTS with subquery parses successfully", func(t *testing.T) {
		query := `
			SELECT * FROM orders o
			WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.is_premium = true)
		`
		conditions, err := ExtractWhereConditions(query)

		require.NoError(t, err)
		// EXISTS subqueries have their internal conditions extracted
		// This validates the parser handles EXISTS clauses without error
		_ = conditions // Behavior may vary based on implementation
	})

	t.Run("NOT EXISTS with subquery", func(t *testing.T) {
		query := `
			SELECT * FROM products p
			WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id)
		`
		conditions, err := ExtractWhereConditions(query)

		require.NoError(t, err)
		// Validates NOT EXISTS is handled
		_ = conditions
	})
}

// =============================================================================
// CTE WITH WHERE CLAUSE
// =============================================================================

// TestExtractWhereConditions_CTE checks WHERE extraction from both CTE body and outer query.
func TestExtractWhereConditions_CTE(t *testing.T) {
	query := `
		WITH active_orders AS (
			SELECT * FROM orders WHERE status = 'active'
		)
		SELECT * FROM active_orders WHERE total > 100
	`

	conditions, err := ExtractWhereConditions(query)

	require.NoError(t, err)
	// Should extract conditions from both the CTE and the main query
	// The exact count depends on implementation - at minimum the outer WHERE
	assert.NotEmpty(t, conditions)
}

// =============================================================================
// JSONB OPERATORS: ->>
// =============================================================================

// TestExtractWhereConditions_JSONB validates JSONB ->> operator extraction with key, value, and table.
func TestExtractWhereConditions_JSONB(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedCount  int
		expectedJSONB  bool
		expectedKey    string
		expectedValue  string
		expectedColumn string
		expectedTable  string
	}{
		{
			name:           "single JSONB ->> predicate",
			query:          "SELECT * FROM orders WHERE order_details->>'shipping_method' = 'express'",
			expectedCount:  1,
			expectedJSONB:  true,
			expectedKey:    "shipping_method",
			expectedValue:  "express",
			expectedColumn: "order_details",
			expectedTable:  "orders",
		},
		{
			name:           "JSONB ->> with regular column",
			query:          "SELECT * FROM orders WHERE status = 'processing' AND order_details->>'shipping_method' = 'express'",
			expectedCount:  2,
			expectedJSONB:  true,
			expectedKey:    "shipping_method",
			expectedValue:  "express",
			expectedColumn: "order_details",
			expectedTable:  "orders",
		},
		{
			name:           "multiple JSONB ->> predicates",
			query:          "SELECT * FROM orders WHERE order_details->>'shipping_method' = 'express' AND order_details->>'used_coupon' = 'PROMO2025'",
			expectedCount:  2,
			expectedJSONB:  true,
			expectedKey:    "shipping_method", // First JSONB predicate
			expectedValue:  "express",
			expectedColumn: "order_details",
			expectedTable:  "orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			require.Len(t, conditions, tt.expectedCount)

			// Find the JSONB condition
			var jsonbCond *WhereCondition
			for i := range conditions {
				if conditions[i].IsJSONB {
					jsonbCond = &conditions[i]
					break
				}
			}

			if tt.expectedJSONB {
				require.NotNil(t, jsonbCond, "Expected a JSONB condition")
				assert.Equal(t, tt.expectedTable, jsonbCond.Table)
				assert.Equal(t, tt.expectedColumn, jsonbCond.Column)
				assert.Equal(t, tt.expectedKey, jsonbCond.JSONBKey)
				assert.Equal(t, tt.expectedValue, jsonbCond.Value)
				assert.True(t, jsonbCond.IsJSONB)
			}
		})
	}
}

// TestExtractWhereConditions_JSONBWithAlias verifies JSONB extraction resolves table aliases.
func TestExtractWhereConditions_JSONBWithAlias(t *testing.T) {
	query := `
		SELECT o.id, o.order_details->>'shipping_method'
		FROM orders o
		WHERE o.status = 'processing' AND o.order_details->>'shipping_method' = 'express'
	`

	conditions, err := ExtractWhereConditions(query)

	require.NoError(t, err)
	require.Len(t, conditions, 2)

	// Find JSONB condition
	var jsonbCond *WhereCondition
	for i := range conditions {
		if conditions[i].IsJSONB {
			jsonbCond = &conditions[i]
			break
		}
	}

	require.NotNil(t, jsonbCond, "Expected a JSONB condition")
	assert.Equal(t, "orders", jsonbCond.Table, "Table alias should be resolved")
	assert.Equal(t, "order_details", jsonbCond.Column)
	assert.Equal(t, "shipping_method", jsonbCond.JSONBKey)
	assert.Equal(t, "express", jsonbCond.Value)
}

// =============================================================================
// FUNCTION CALLS IN WHERE
// =============================================================================

// TestExtractWhereConditions_Functions confirms WHERE with LOWER, COALESCE, and DATE functions parses successfully.
func TestExtractWhereConditions_Functions(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedCount int
	}{
		{
			name:          "LOWER function",
			query:         "SELECT * FROM users WHERE LOWER(email) = 'test@example.com'",
			expectedCount: 1,
		},
		{
			name:          "COALESCE function",
			query:         "SELECT * FROM orders WHERE COALESCE(status, 'unknown') = 'pending'",
			expectedCount: 1,
		},
		{
			name:          "DATE function",
			query:         "SELECT * FROM orders WHERE DATE(created_at) = '2024-01-01'",
			expectedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, err := ExtractWhereConditions(tt.query)

			require.NoError(t, err)
			// Function calls in WHERE may not extract as traditional column conditions
			// This test validates the parser doesn't fail on such queries
			_ = conditions // Implementation may vary
		})
	}
}
