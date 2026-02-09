package postgresparser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSQLSimpleSelect(t *testing.T) {
	sql := "SELECT id, name FROM users WHERE status = ?;"
	pq, err := ParseSQL(sql)
	require.NoError(t, err, "ParseSQL returned error")
	assert.Equal(t, QueryCommandSelect, pq.Command, "expected command SELECT")
	require.Len(t, pq.Columns, 2, "expected 2 columns")
	assert.Equal(t, "id", pq.Columns[0].Expression, "unexpected column 1")
	assert.Equal(t, "name", pq.Columns[1].Expression, "unexpected column 2")
	require.Len(t, pq.Tables, 1, "expected 1 table")
	assert.Equal(t, "users", pq.Tables[0].Name, "unexpected table name")
	assert.Equal(t, TableTypeBase, pq.Tables[0].Type, "unexpected table type")
	require.Len(t, pq.Where, 1, "expected 1 WHERE clause")
	assert.Equal(t, "status=?", normalise(pq.Where[0]), "unexpected WHERE clause")
	require.Len(t, pq.Parameters, 1, "expected 1 parameter")
	assert.Equal(t, "?", pq.Parameters[0].Raw, "unexpected parameter raw")
	assert.Equal(t, 1, pq.Parameters[0].Position, "unexpected parameter position")
}

func TestParseSQLJoinWithAlias(t *testing.T) {
	sql := `
SELECT o.id, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.created_at > $1 AND c.active = true;`

	pq, err := ParseSQL(sql)
	require.NoError(t, err, "ParseSQL returned error")
	require.Len(t, pq.Tables, 2, "expected 2 tables")

	assert.Equal(t, "orders", pq.Tables[0].Name, "unexpected first table name")
	assert.Equal(t, "o", pq.Tables[0].Alias, "unexpected first table alias")

	assert.Equal(t, "customers", pq.Tables[1].Name, "unexpected second table name")
	assert.Equal(t, "c", pq.Tables[1].Alias, "unexpected second table alias")

	require.NotEmpty(t, pq.JoinConditions, "expected join condition")
	assert.Contains(t, normalise(pq.JoinConditions[0]), "o.customer_id=c.id", "expected join condition content")

	require.Len(t, pq.Where, 1, "expected 1 WHERE clause")
	assert.Contains(t, normalise(pq.Where[0]), "o.created_at>$1", "unexpected WHERE clause")

	require.Len(t, pq.Parameters, 1, "expected 1 parameter")
	assert.Equal(t, "$1", pq.Parameters[0].Raw, "unexpected parameter raw")
	assert.Equal(t, 1, pq.Parameters[0].Position, "unexpected parameter position")
}

func TestParseSQLWithCTE(t *testing.T) {
	sql := `
WITH ranked AS (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY tenant ORDER BY created_at DESC) AS seq
    FROM orders
    WHERE tenant = ?
)
SELECT r.id, r.seq
FROM ranked r
WHERE r.seq <= 5;`

	pq, err := ParseSQL(sql)
	require.NoError(t, err, "ParseSQL returned error")
	require.Len(t, pq.CTEs, 1, "expected 1 CTE")
	assert.Equal(t, "ranked", strings.ToLower(pq.CTEs[0].Name), "unexpected CTE name")

	// Should now have both the base table "orders" from the CTE and the CTE reference "ranked"
	require.Len(t, pq.Tables, 2, "expected 2 tables (orders from CTE and ranked ref)")

	foundOrders := false
	foundRanked := false
	for _, tbl := range pq.Tables {
		if tbl.Name == "orders" && tbl.Type == TableTypeBase {
			foundOrders = true
		}
		if strings.ToLower(tbl.Name) == "ranked" && tbl.Type == TableTypeCTE {
			foundRanked = true
		}
	}
	assert.True(t, foundOrders, "expected orders base table from CTE")
	assert.True(t, foundRanked, "expected ranked CTE reference")

	require.Len(t, pq.Where, 1, "expected 1 WHERE clause")
	assert.Equal(t, "r.seq<=5", normalise(pq.Where[0]), "unexpected WHERE clause")

	require.Len(t, pq.Parameters, 1, "expected 1 parameter")
	assert.Equal(t, "?", pq.Parameters[0].Raw, "unexpected parameter raw")
}

func TestParseSQLGroupOrderLimit(t *testing.T) {
	sql := `
SELECT tenant, COUNT(*) AS cnt
FROM events
WHERE occurred_at >= $1
GROUP BY tenant
HAVING COUNT(*) > 5
ORDER BY cnt DESC NULLS LAST
LIMIT 10 OFFSET 2;`

	pq, err := ParseSQL(sql)
	require.NoError(t, err, "ParseSQL returned error")

	require.Len(t, pq.GroupBy, 1, "expected 1 GROUP BY")
	assert.Equal(t, "tenant", pq.GroupBy[0], "unexpected GROUP BY")

	require.Len(t, pq.Having, 1, "expected 1 HAVING")
	assert.Contains(t, normalise(pq.Having[0]), "count(*)>5", "unexpected HAVING")

	require.Len(t, pq.OrderBy, 1, "expected 1 ORDER BY")
	assert.Equal(t, "DESC", strings.ToUpper(pq.OrderBy[0].Direction), "unexpected ORDER BY direction")
	assert.Equal(t, "NULLS LAST", strings.ToUpper(pq.OrderBy[0].Nulls), "unexpected ORDER BY nulls")

	require.NotNil(t, pq.Limit, "expected LIMIT clause")
	assert.Contains(t, strings.ToUpper(pq.Limit.Limit), "LIMIT", "expected LIMIT clause content")
	assert.Contains(t, strings.ToUpper(pq.Limit.Offset), "OFFSET", "expected OFFSET clause content")
}

func TestParseSQLFunctionAndSubqueryTables(t *testing.T) {
	sql := `
SELECT sub.id, ids.id
FROM (SELECT id FROM accounts) sub
CROSS JOIN LATERAL unnest($1) AS ids(id);`

	pq, err := ParseSQL(sql)
	require.NoError(t, err, "ParseSQL returned error")

	var foundSubquery, foundFunction, foundBase bool
	for _, tbl := range pq.Tables {
		switch tbl.Type {
		case TableTypeSubquery:
			if strings.ToLower(tbl.Alias) == "sub" {
				foundSubquery = true
			}
		case TableTypeFunction:
			if strings.ToLower(tbl.Alias) == "ids" {
				foundFunction = true
			}
		case TableTypeBase:
			if strings.ToLower(tbl.Name) == "accounts" {
				foundBase = true
			}
		}
	}
	assert.True(t, foundSubquery, "expected subquery table reference")
	assert.True(t, foundFunction, "expected function table reference")
	assert.True(t, foundBase, "expected base accounts table to be surfaced")

	require.Len(t, pq.Parameters, 1, "expected 1 parameter")
	assert.Equal(t, "$1", pq.Parameters[0].Raw, "unexpected parameter raw")
}
