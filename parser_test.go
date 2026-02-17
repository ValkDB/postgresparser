package postgresparser

import (
	"errors"
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

const multiCreateTableSQL = `
CREATE TABLE public.api_key (
    id integer NOT NULL
);
CREATE TABLE public.sometable (
    id integer NOT NULL
);`

func TestParseSQLTable(t *testing.T) {
	tests := []struct {
		name             string
		sql              string
		wantErrIs        error
		wantParseErrType bool
		wantCommand      QueryCommand
		assertQuery      func(t *testing.T, q *ParsedQuery)
	}{
		{
			name:        "single statement",
			sql:         "SELECT 1",
			wantCommand: QueryCommandSelect,
		},
		{
			name:        "trailing semicolon",
			sql:         "SELECT 1;",
			wantCommand: QueryCommandSelect,
		},
		{
			name:        "legacy first statement behavior",
			sql:         "SELECT $1 AS first; SELECT $2 AS second;",
			wantCommand: QueryCommandSelect,
			assertQuery: func(t *testing.T, q *ParsedQuery) {
				t.Helper()
				assert.Contains(t, q.RawSQL, "SELECT $2 AS second")
				require.Len(t, q.Parameters, 2)
			},
		},
		{
			name:        "legacy create table first statement behavior",
			sql:         multiCreateTableSQL,
			wantCommand: QueryCommandDDL,
			assertQuery: func(t *testing.T, q *ParsedQuery) {
				t.Helper()
				require.Len(t, q.DDLActions, 1)
				assert.Equal(t, DDLCreateTable, q.DDLActions[0].Type)
				assert.Equal(t, "api_key", q.DDLActions[0].ObjectName)
				assert.Contains(t, q.RawSQL, "CREATE TABLE public.sometable")
			},
		},
		{
			name:             "invalid sql",
			sql:              "SELECT FROM",
			wantParseErrType: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q, err := ParseSQL(tc.sql)

			if tc.wantErrIs != nil || tc.wantParseErrType {
				require.Error(t, err)
				assert.Nil(t, q)
				if tc.wantErrIs != nil {
					assert.ErrorIs(t, err, tc.wantErrIs)
				}
				if tc.wantParseErrType {
					var parseErr *ParseErrors
					assert.True(t, errors.As(err, &parseErr))
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, q)
			assert.Equal(t, tc.wantCommand, q.Command)
			if tc.assertQuery != nil {
				tc.assertQuery(t, q)
			}
		})
	}
}

func TestParseSQLAllTable(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		wantErrIs     error
		wantStmtCount int
		wantFailed    bool
		wantCommands  []QueryCommand
		wantWarnCodes [][]ParseWarningCode
		assertBatch   func(t *testing.T, batch *ParseBatchResult)
	}{
		{
			name:          "single statement select",
			sql:           "SELECT 1",
			wantStmtCount: 1,
			wantFailed:    false,
			wantCommands:  []QueryCommand{QueryCommandSelect},
		},
		{
			name:          "trailing semicolon",
			sql:           "SELECT 1;",
			wantStmtCount: 1,
			wantFailed:    false,
			wantCommands:  []QueryCommand{QueryCommandSelect},
		},
		{
			name:          "trailing double semicolon",
			sql:           "SELECT 1;;",
			wantStmtCount: 1,
			wantFailed:    false,
			wantCommands:  []QueryCommand{QueryCommandSelect},
		},
		{
			name:          "mixed multi statement",
			sql:           "SET client_min_messages = warning; SELECT $1 AS v; CREATE TABLE public.t(id integer);",
			wantStmtCount: 3,
			wantFailed:    false,
			wantCommands:  []QueryCommand{QueryCommandUnknown, QueryCommandSelect, QueryCommandDDL},
		},
		{
			name:          "two create table statements",
			sql:           multiCreateTableSQL,
			wantStmtCount: 2,
			wantFailed:    false,
			wantCommands:  []QueryCommand{QueryCommandDDL, QueryCommandDDL},
			assertBatch: func(t *testing.T, batch *ParseBatchResult) {
				t.Helper()
				require.Len(t, batch.Statements[0].Query.DDLActions, 1)
				require.Len(t, batch.Statements[1].Query.DDLActions, 1)
				assert.Equal(t, "api_key", batch.Statements[0].Query.DDLActions[0].ObjectName)
				assert.Equal(t, "sometable", batch.Statements[1].Query.DDLActions[0].ObjectName)
			},
		},
		{
			name:          "invalid sql single statement has statement warning",
			sql:           "SELECT FROM",
			wantStmtCount: 1,
			wantFailed:    true,
			wantCommands:  []QueryCommand{QueryCommandSelect},
			wantWarnCodes: [][]ParseWarningCode{
				{ParseWarningCodeSyntaxError},
			},
		},
		{
			name:          "invalid sql mid-batch warning is attached to statement index",
			sql:           "SELECT 1;\nSELECT FROM;\nSELECT 2;",
			wantStmtCount: 3,
			wantFailed:    true,
			wantCommands:  []QueryCommand{QueryCommandSelect, QueryCommandSelect, QueryCommandSelect},
			wantWarnCodes: [][]ParseWarningCode{
				nil,
				{ParseWarningCodeSyntaxError},
				nil,
			},
		},
		{
			name:          "complex mixed statements",
			sql:           `SELECT u.id, COUNT(o.id) AS order_count FROM users u LEFT JOIN orders o ON o.user_id = u.id WHERE u.active = true AND o.created_at > $1 GROUP BY u.id HAVING COUNT(o.id) > 1 ORDER BY order_count DESC LIMIT 10; UPDATE users SET status = 'active' WHERE id = $2 RETURNING id; DELETE FROM sessions WHERE expires_at < NOW();`,
			wantStmtCount: 3,
			wantFailed:    false,
			wantCommands:  []QueryCommand{QueryCommandSelect, QueryCommandUpdate, QueryCommandDelete},
			assertBatch: func(t *testing.T, batch *ParseBatchResult) {
				t.Helper()
				selectQ := batch.Statements[0].Query
				assert.GreaterOrEqual(t, len(selectQ.Tables), 2)
				assert.NotEmpty(t, selectQ.JoinConditions)
				assert.NotEmpty(t, selectQ.Where)
				assert.NotEmpty(t, selectQ.GroupBy)
				assert.NotEmpty(t, selectQ.Having)
				assert.NotEmpty(t, selectQ.OrderBy)
				assert.NotNil(t, selectQ.Limit)
				require.Len(t, selectQ.Parameters, 1)
				assert.Equal(t, "$1", selectQ.Parameters[0].Raw)

				updateQ := batch.Statements[1].Query
				assert.NotEmpty(t, updateQ.SetClauses)
				assert.NotEmpty(t, updateQ.Where)
				assert.NotEmpty(t, updateQ.Returning)
				require.Len(t, updateQ.Parameters, 1)
				assert.Equal(t, "$2", updateQ.Parameters[0].Raw)

				deleteQ := batch.Statements[2].Query
				assert.NotEmpty(t, deleteQ.Where)
				assert.Empty(t, deleteQ.Parameters)
			},
		},
		{
			name:      "empty input",
			sql:       " \n\t ",
			wantErrIs: ErrNoStatements,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batch, err := ParseSQLAll(tc.sql)

			if tc.wantErrIs != nil {
				require.Error(t, err)
				assert.Nil(t, batch)
				assert.ErrorIs(t, err, tc.wantErrIs)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, batch)
			assert.Equal(t, tc.wantFailed, batch.HasFailures)
			require.Len(t, batch.Statements, tc.wantStmtCount)
			for i := range batch.Statements {
				assert.Equal(t, i+1, batch.Statements[i].Index)
			}

			if tc.wantCommands != nil {
				require.Len(t, tc.wantCommands, len(batch.Statements))
				for i := range tc.wantCommands {
					require.NotNil(t, batch.Statements[i].Query)
					assert.Equal(t, tc.wantCommands[i], batch.Statements[i].Query.Command)
				}
			}

			if tc.wantWarnCodes != nil {
				require.Len(t, tc.wantWarnCodes, len(batch.Statements))
				for i := range tc.wantWarnCodes {
					codes := make([]ParseWarningCode, 0, len(batch.Statements[i].Warnings))
					for _, w := range batch.Statements[i].Warnings {
						codes = append(codes, w.Code)
					}
					expected := tc.wantWarnCodes[i]
					if expected == nil {
						expected = []ParseWarningCode{}
					}
					assert.Equal(t, expected, codes)
				}
			}

			if tc.assertBatch != nil {
				tc.assertBatch(t, batch)
			}
		})
	}
}

func TestParseSQLStrictTable(t *testing.T) {
	tests := []struct {
		name             string
		sql              string
		wantErrIs        error
		wantParseErrType bool
		wantCommand      QueryCommand
		wantStmtCount    int
	}{
		{
			name:        "single select",
			sql:         "SELECT 1",
			wantCommand: QueryCommandSelect,
		},
		{
			name:        "single select trailing semicolon",
			sql:         "SELECT 1;",
			wantCommand: QueryCommandSelect,
		},
		{
			name:        "single ddl",
			sql:         "CREATE TABLE users(id integer)",
			wantCommand: QueryCommandDDL,
		},
		{
			name:          "multi statement",
			sql:           "SELECT 1; SELECT 2",
			wantErrIs:     ErrMultipleStatements,
			wantStmtCount: 2,
		},
		{
			name:      "no statements",
			sql:       " \n\t ",
			wantErrIs: ErrNoStatements,
		},
		{
			name:             "invalid sql",
			sql:              "SELECT FROM",
			wantParseErrType: true,
		},
		{
			name:             "invalid sql mid batch",
			sql:              "SELECT 1; SELECT FROM; SELECT 2",
			wantParseErrType: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ParseSQLStrict(tc.sql)

			if tc.wantErrIs != nil || tc.wantParseErrType {
				require.Error(t, err)
				assert.Nil(t, result)
				if tc.wantErrIs != nil {
					assert.ErrorIs(t, err, tc.wantErrIs)
				}
				if tc.wantStmtCount > 0 {
					var strictErr *MultipleStatementsError
					require.True(t, errors.As(err, &strictErr))
					assert.Equal(t, tc.wantStmtCount, strictErr.StatementCount)
				}
				if tc.wantParseErrType {
					var parseErr *ParseErrors
					assert.True(t, errors.As(err, &parseErr))
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, tc.wantCommand, result.Command)
		})
	}
}
