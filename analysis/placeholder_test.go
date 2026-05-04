package analysis

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlaceholders(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []Placeholder
	}{
		{"where eq", "SELECT * FROM t WHERE id = ?", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: "id"},
		}},
		{"where range two columns", "SELECT * FROM t WHERE created_at >= ? AND amount > ?", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: "created_at"},
			{Index: 2, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: "amount"},
		}},
		{"limit only", "SELECT * FROM t LIMIT ?", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleLimit},
		}},
		{"limit and offset", "SELECT * FROM t LIMIT ? OFFSET ?", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleLimit},
			{Index: 2, Style: "?", Role: PlaceholderRoleOffset},
		}},
		{"group by ordinals", "SELECT a, b FROM t GROUP BY ?, ?", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleGroupByOrdinal},
			{Index: 2, Style: "?", Role: PlaceholderRoleGroupByOrdinal},
		}},
		{"order by mixed dir", "SELECT a, b FROM t ORDER BY ?, ? DESC", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleOrderByOrdinal},
			{Index: 2, Style: "?", Role: PlaceholderRoleOrderByOrdinal},
		}},
		{"date trunc unit", "SELECT date_trunc(?, created_at) FROM t", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleFunctionArg, ParentFn: &FunctionRef{Name: "date_trunc", ArgIndex: 0, ArgCount: 2}},
		}},
		{"extract field", "SELECT extract(? FROM created_at) FROM t", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleFunctionArg, ParentFn: &FunctionRef{Name: "extract", ArgIndex: 0, ArgCount: 2}},
		}},
		{"length wrapped column", "SELECT * FROM t WHERE length(notes) < ?", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: "notes"},
		}},
		{"complex wrapped column", "SELECT * FROM t WHERE length(notes || '_x') < ?", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: ""},
		}},
		{"coalesce wrapped column", "SELECT * FROM t WHERE coalesce(status, 'unknown') = ?", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: "status"},
		}},
		{"interval operand", "SELECT * FROM t WHERE created_at > now() - INTERVAL ?", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleIntervalOperand},
		}},
		{"insert values", "INSERT INTO t (a, b, c) VALUES (?, ?, ?)", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleInsertValue, InsertColumn: "a"},
			{Index: 2, Style: "?", Role: PlaceholderRoleInsertValue, InsertColumn: "b"},
			{Index: 3, Style: "?", Role: PlaceholderRoleInsertValue, InsertColumn: "c"},
		}},
		{"insert values without column list", "INSERT INTO t VALUES (?, ?, ?)", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleInsertValue},
			{Index: 2, Style: "?", Role: PlaceholderRoleInsertValue},
			{Index: 3, Style: "?", Role: PlaceholderRoleInsertValue},
		}},
		{"update set", "UPDATE t SET name = ?, age = ? WHERE id = ?", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleUpdateSetValue, UpdateColumn: "name"},
			{Index: 2, Style: "?", Role: PlaceholderRoleUpdateSetValue, UpdateColumn: "age"},
			{Index: 3, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: "id"},
		}},
		{"in list", "SELECT * FROM t WHERE col IN (?, ?, ?)", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleInListMember, ColumnRef: "col"},
			{Index: 2, Style: "?", Role: PlaceholderRoleInListMember, ColumnRef: "col"},
			{Index: 3, Style: "?", Role: PlaceholderRoleInListMember, ColumnRef: "col"},
		}},
		{"any array", "SELECT * FROM t WHERE col = ANY(ARRAY[?, ?, ?])", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleArrayMember, ColumnRef: "col"},
			{Index: 2, Style: "?", Role: PlaceholderRoleArrayMember, ColumnRef: "col"},
			{Index: 3, Style: "?", Role: PlaceholderRoleArrayMember, ColumnRef: "col"},
		}},
		{"between", "SELECT * FROM t WHERE col BETWEEN ? AND ?", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleBetweenLow, ColumnRef: "col"},
			{Index: 2, Style: "?", Role: PlaceholderRoleBetweenHigh, ColumnRef: "col"},
		}},
		{"having agg", "SELECT id, count(*) FROM t GROUP BY id HAVING count(*) > ?", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleHavingValue},
		}},
		{"case bucket", "SELECT CASE WHEN amount >= ? THEN ? ELSE ? END FROM t", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleCaseExpr, CaseClause: CaseClausePredicate, ColumnRef: "amount"},
			{Index: 2, Style: "?", Role: PlaceholderRoleCaseExpr, CaseClause: CaseClauseResult},
			{Index: 3, Style: "?", Role: PlaceholderRoleCaseExpr, CaseClause: CaseClauseDefault},
		}},
		{"dollar style", "SELECT * FROM t WHERE id = $1 AND status = $2", []Placeholder{
			{Index: 1, Style: "$", Role: PlaceholderRoleWhereValue, ColumnRef: "id"},
			{Index: 2, Style: "$", Role: PlaceholderRoleWhereValue, ColumnRef: "status"},
		}},
		{"jsonb exists", "SELECT * FROM t WHERE data ? 'key'", []Placeholder{}},
		{"jsonb exists any", "SELECT * FROM t WHERE data ?| array['a','b']", []Placeholder{}},
		{"jsonb exists all", "SELECT * FROM t WHERE data ?& array['a','b']", []Placeholder{}},
		{"question in literal", "SELECT * FROM t WHERE notes = 'has a ?'", []Placeholder{}},
		{"escaped quote with question", `SELECT * FROM t WHERE notes = 'don''t mark me ?'`, []Placeholder{}},
		{"line comment", "SELECT * FROM t -- ? this is a comment\nWHERE id = ?", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: "id"},
		}},
		{"block comment", "SELECT * /* ?,?,? */ FROM t WHERE id = ?", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: "id"},
		}},
		{"cte with placeholder", `WITH active AS (SELECT id FROM t WHERE status = ?) SELECT * FROM active WHERE id < ?`, []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: "status"},
			{Index: 2, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: "id"},
		}},
		{"cte union with function predicates", `WITH named AS (SELECT id FROM a WHERE lower(name) = ?)
		 SELECT id FROM named WHERE id IN (SELECT id FROM b WHERE status = ?)
		 UNION ALL
		 SELECT id FROM c WHERE length(notes) < ?`, []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: "name"},
			{Index: 2, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: "status"},
			{Index: 3, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: "notes"},
		}},
		{"lateral limit", "SELECT * FROM t, LATERAL (SELECT * FROM u WHERE u.tid = t.id LIMIT ?) sub", []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleLimit},
		}},
		{"normalized aggregation", `SELECT date_trunc(?, o.created_at) AS week, c.plan, count(DISTINCT o.id), sum(oi.unit_price * oi.quantity)
		 FROM orders o
		 JOIN customers c ON c.id = o.customer_id
		 JOIN order_items oi ON oi.order_id = o.id
		 WHERE o.created_at >= ? AND o.created_at < ? AND length(o.notes) < ?
		 GROUP BY ?, ?
		 LIMIT ?`, []Placeholder{
			{Index: 1, Style: "?", Role: PlaceholderRoleFunctionArg, ParentFn: &FunctionRef{Name: "date_trunc", ArgIndex: 0, ArgCount: 2}},
			{Index: 2, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: "o.created_at"},
			{Index: 3, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: "o.created_at"},
			{Index: 4, Style: "?", Role: PlaceholderRoleWhereValue, ColumnRef: "o.notes"},
			{Index: 5, Style: "?", Role: PlaceholderRoleGroupByOrdinal},
			{Index: 6, Style: "?", Role: PlaceholderRoleGroupByOrdinal},
			{Index: 7, Style: "?", Role: PlaceholderRoleLimit},
		}},
		{"upsert placeholders", "INSERT INTO accounts (email, plan) VALUES ($1, $2) ON CONFLICT (email) DO UPDATE SET plan = $3 WHERE accounts.email = $4", []Placeholder{
			{Index: 1, Style: "$", Role: PlaceholderRoleInsertValue, InsertColumn: "email"},
			{Index: 2, Style: "$", Role: PlaceholderRoleInsertValue, InsertColumn: "plan"},
			{Index: 3, Style: "$", Role: PlaceholderRoleUpdateSetValue, UpdateColumn: "plan"},
			{Index: 4, Style: "$", Role: PlaceholderRoleWhereValue, ColumnRef: "accounts.email"},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := AnalyzeSQL(tc.sql)
			require.NoError(t, err)
			require.Len(t, result.Placeholders, len(tc.want))
			for i, want := range tc.want {
				got := result.Placeholders[i]
				require.Equal(t, want.Index, got.Index, "placeholder %d index", i)
				require.Equal(t, want.Style, got.Style, "placeholder %d style", i)
				require.Equal(t, want.Role, got.Role, "placeholder %d role", i)
				require.Equal(t, want.ColumnRef, got.ColumnRef, "placeholder %d column ref", i)
				require.Equal(t, want.CaseClause, got.CaseClause, "placeholder %d case clause", i)
				require.Equal(t, want.InsertColumn, got.InsertColumn, "placeholder %d insert column", i)
				require.Equal(t, want.UpdateColumn, got.UpdateColumn, "placeholder %d update column", i)
				if want.ParentFn == nil {
					require.Nil(t, got.ParentFn, "placeholder %d parent function", i)
					continue
				}
				require.NotNil(t, got.ParentFn, "placeholder %d parent function", i)
				require.Equal(t, want.ParentFn.Name, got.ParentFn.Name, "placeholder %d function name", i)
				require.Equal(t, want.ParentFn.ArgIndex, got.ParentFn.ArgIndex, "placeholder %d function arg index", i)
				require.Equal(t, want.ParentFn.ArgCount, got.ParentFn.ArgCount, "placeholder %d function arg count", i)
			}
		})
	}
}
