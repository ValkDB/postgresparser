package analysis

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAnalyze_OperatorExtraction validates individual operator extraction from WHERE predicates.
func TestAnalyze_OperatorExtraction(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantOps []string
	}{
		{
			name:    "equality operator",
			sql:     `SELECT * FROM users WHERE id = 123`,
			wantOps: []string{"="},
		},
		{
			name:    "comparison operators",
			sql:     `SELECT * FROM products WHERE price > 100 AND stock <= 50`,
			wantOps: []string{">", "<="},
		},
		{
			name:    "LIKE operator",
			sql:     `SELECT * FROM users WHERE name LIKE '%john%'`,
			wantOps: []string{"LIKE"},
		},
		{
			name:    "IN operator",
			sql:     `SELECT * FROM orders WHERE status IN ('pending', 'shipped')`,
			wantOps: []string{"IN"},
		},
		{
			name:    "IS NULL operator",
			sql:     `SELECT * FROM users WHERE deleted_at IS NULL`,
			wantOps: []string{"IS"},
		},
		{
			name:    "mixed operators",
			sql:     `SELECT * FROM products WHERE price >= 10 AND price < 100 AND category != 'archived'`,
			wantOps: []string{">=", "<", "!="},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := AnalyzeSQL(tt.sql)
			require.NoError(t, err)
			require.NotNil(t, result)

			// Collect unique operators (since we only want one per expression)
			operatorSet := make(map[string]bool)
			for _, usage := range result.ColumnUsage {
				if usage.Operator != "" {
					operatorSet[usage.Operator] = true
				}
			}
			var operators []string
			for op := range operatorSet {
				operators = append(operators, op)
			}
			assert.ElementsMatch(t, tt.wantOps, operators, "Operators should match")
		})
	}
}

// TestAnalyze_FunctionUsage verifies aggregate and window function names in ColumnUsage.
func TestAnalyze_FunctionUsage(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantFuncs []string
	}{
		{
			name:      "SUM function",
			sql:       `SELECT SUM(total) FROM orders`,
			wantFuncs: []string{"SUM"},
		},
		{
			name:      "multiple aggregate functions",
			sql:       `SELECT AVG(price), MAX(price), MIN(price) FROM products`,
			wantFuncs: []string{"AVG", "MAX", "MIN"},
		},
		{
			name:      "window functions",
			sql:       `SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) FROM employees`,
			wantFuncs: []string{"ROW_NUMBER"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := AnalyzeSQL(tt.sql)
			require.NoError(t, err)
			require.NotNil(t, result)

			var functions []string
			seen := make(map[string]bool)
			for _, usage := range result.ColumnUsage {
				for _, fn := range usage.Functions {
					if !seen[fn] {
						functions = append(functions, fn)
						seen[fn] = true
					}
				}
			}
			assert.ElementsMatch(t, tt.wantFuncs, functions, "Functions should match")
		})
	}
}

// TestAnalyze_LimitContext checks LIMIT/OFFSET extraction and nested flag.
func TestAnalyze_LimitContext(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantLimit  string
		wantOffset string
		wantNested bool
	}{
		{
			name:       "simple limit",
			sql:        `SELECT * FROM users LIMIT 10`,
			wantLimit:  "LIMIT 10",
			wantOffset: "",
			wantNested: false,
		},
		{
			name:       "limit with offset",
			sql:        `SELECT * FROM users LIMIT 10 OFFSET 20`,
			wantLimit:  "LIMIT 10",
			wantOffset: "OFFSET 20",
			wantNested: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := AnalyzeSQL(tt.sql)
			require.NoError(t, err)
			require.NotNil(t, result)

			if tt.wantLimit != "" || tt.wantOffset != "" {
				require.NotNil(t, result.Limit, "Limit should be present")
				assert.Equal(t, tt.wantLimit, result.Limit.Limit)
				assert.Equal(t, tt.wantOffset, result.Limit.Offset)
				assert.Equal(t, tt.wantNested, result.Limit.IsNested)
			} else {
				assert.Nil(t, result.Limit)
			}
		})
	}
}

// TestAnalyze_DerivedColumns validates alias-to-expression lineage tracking.
func TestAnalyze_DerivedColumns(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		wantMappings map[string]string
	}{
		{
			name: "simple alias",
			sql:  `SELECT COUNT(*) AS order_count FROM orders`,
			wantMappings: map[string]string{
				"order_count": "COUNT(*)",
			},
		},
		{
			name: "multiple aliases",
			sql:  `SELECT SUM(total) AS total_revenue, AVG(total) AS avg_order_value FROM orders`,
			wantMappings: map[string]string{
				"total_revenue":   "SUM(total)",
				"avg_order_value": "AVG(total)",
			},
		},
		{
			name: "expression alias",
			sql:  `SELECT price * quantity AS line_total FROM order_items`,
			wantMappings: map[string]string{
				"line_total": "price * quantity",
			},
		},
		{
			name: "case expression alias",
			sql:  `SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END AS is_active FROM users`,
			wantMappings: map[string]string{
				"is_active": "CASE WHEN status = 'active' THEN 1 ELSE 0 END",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := AnalyzeSQL(tt.sql)
			require.NoError(t, err)
			require.NotNil(t, result)

			if tt.wantMappings != nil {
				require.NotNil(t, result.DerivedColumns, "DerivedColumns should be present")
				for alias, expr := range tt.wantMappings {
					// Note: expressions might have whitespace removed by parser
					actualExpr := result.DerivedColumns[alias]
					assert.Contains(t, actualExpr, expr, "Expression for alias %s should match", alias)
				}
			}
		})
	}
}

// TestAnalyze_GroupByHaving verifies GROUP BY column and HAVING clause extraction.
func TestAnalyze_GroupByHaving(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantGroupBy []string
		wantHaving  []string
	}{
		{
			name:        "simple GROUP BY",
			sql:         `SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id`,
			wantGroupBy: []string{"customer_id"},
			wantHaving:  nil,
		},
		{
			name:        "GROUP BY with HAVING",
			sql:         `SELECT customer_id, SUM(total) FROM orders GROUP BY customer_id HAVING SUM(total) > 1000`,
			wantGroupBy: []string{"customer_id"},
			wantHaving:  []string{"SUM(total) > 1000"},
		},
		{
			name:        "multiple GROUP BY columns",
			sql:         `SELECT category, subcategory, COUNT(*) FROM products GROUP BY category, subcategory`,
			wantGroupBy: []string{"category", "subcategory"},
			wantHaving:  nil,
		},
		{
			name:        "complex HAVING",
			sql:         `SELECT dept, AVG(salary) FROM employees GROUP BY dept HAVING AVG(salary) > 50000 AND COUNT(*) > 5`,
			wantGroupBy: []string{"dept"},
			wantHaving:  []string{"AVG(salary) > 50000 AND COUNT(*) > 5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := AnalyzeSQL(tt.sql)
			require.NoError(t, err)
			require.NotNil(t, result)

			if tt.wantGroupBy != nil {
				assert.ElementsMatch(t, tt.wantGroupBy, result.GroupBy, "GROUP BY columns should match")
			}
			if tt.wantHaving != nil {
				assert.ElementsMatch(t, tt.wantHaving, result.Having, "HAVING clauses should match")
			}
		})
	}
}

// TestAnalyze_LateralCorrelation checks LATERAL join correlation detection.
func TestAnalyze_LateralCorrelation(t *testing.T) {
	tests := []struct {
		name            string
		sql             string
		wantCorrelated  bool
		correlationType string
	}{
		{
			name: "CROSS JOIN LATERAL",
			sql: `SELECT * FROM products p
				  CROSS JOIN LATERAL unnest(p.tags) AS tag(name)`,
			wantCorrelated:  true,
			correlationType: "LATERAL",
		},
		{
			name: "regular join (no correlation)",
			sql: `SELECT * FROM users u
				  JOIN orders o ON o.user_id = u.id`,
			wantCorrelated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := AnalyzeSQL(tt.sql)
			require.NoError(t, err)
			require.NotNil(t, result)

			if tt.wantCorrelated {
				require.NotNil(t, result.Correlations, "Correlations should be present")
				require.Greater(t, len(result.Correlations), 0, "Should have at least one correlation")
				assert.Equal(t, tt.correlationType, result.Correlations[0].Type)
			} else if result.Correlations != nil {
				assert.Empty(t, result.Correlations, "Should have no correlations")
			}
		})
	}
}
