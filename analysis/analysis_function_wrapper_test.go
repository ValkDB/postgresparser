package analysis

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLColumnUsage_FunctionWrapperSurvivesConversion(t *testing.T) {
	cases := []struct {
		name     string
		sql      string
		column   string
		funcName string
		cast     string
		nested   bool
	}{
		{"length", "SELECT * FROM t WHERE length(notes) < 100", "notes", "length", "", false},
		{"date_trunc", "SELECT * FROM t WHERE date_trunc('month', created_at) > '2025-01-01'", "created_at", "date_trunc", "", false},
		{"extract", "SELECT * FROM t WHERE extract(year FROM created_at) = 2025", "created_at", "extract", "", false},
		{"coalesce", "SELECT * FROM t WHERE coalesce(name, '') <> ''", "name", "coalesce", "", false},
		{"nested lower", "SELECT * FROM t WHERE lower(lower(name)) = 'foo'", "name", "lower", "", true},
		{"cast", "SELECT * FROM t WHERE length(col)::int < 5", "col", "length", "int", false},
		{"pg_catalog canonicalised", "SELECT * FROM t WHERE pg_catalog.length(name) > 5", "name", "length", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, err := AnalyzeSQL(tc.sql)
			require.NoError(t, err)
			var found *SQLColumnUsage
			for i := range a.ColumnUsage {
				if a.ColumnUsage[i].Column == tc.column && a.ColumnUsage[i].Function != nil {
					found = &a.ColumnUsage[i]
					break
				}
			}
			require.NotNil(t, found, "expected SQLColumnUsage with non-nil Function for column %q; got: %+v", tc.column, a.ColumnUsage)
			require.Equal(t, tc.funcName, found.Function.Name)
			require.Equal(t, tc.cast, found.Function.Cast)
			require.Equal(t, tc.nested, found.Function.IsNested)
		})
	}
}

func TestSQLColumnUsage_NoFunctionForNonWHEREUsages(t *testing.T) {
	// HAVING/JOIN ON/ORDER BY etc. analysed via AnalyzeSQL must NEVER carry a
	// non-nil Function on their SQLColumnUsage entries.
	cases := []string{
		"SELECT id FROM t GROUP BY id HAVING length(name) < 5",
		"SELECT * FROM a JOIN b ON lower(a.x) = lower(b.y)",
		"SELECT * FROM t ORDER BY lower(name)",
		"SELECT length(name) FROM t WHERE id = 1",
		"UPDATE t SET status = 'x' WHERE id = 1 RETURNING length(name)",
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			a, err := AnalyzeSQL(sql)
			require.NoError(t, err)
			for _, u := range a.ColumnUsage {
				if u.UsageType != SQLUsageTypeFilter {
					require.Nil(t, u.Function,
						"non-filter usage must not carry Function: %+v", u)
				}
			}
		})
	}
}
