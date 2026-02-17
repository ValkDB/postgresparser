package postgresparser

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const multiCreateTableSQL = `
CREATE TABLE public.api_key (
    id integer NOT NULL
);
CREATE TABLE public.sometable (
    id integer NOT NULL
);`

func TestParseSQLBackwardCompatibleFirstStatementBehavior(t *testing.T) {
	sql := "SELECT $1 AS first; SELECT $2 AS second;"

	result, err := ParseSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, QueryCommandSelect, result.Command)
	assert.Contains(t, result.RawSQL, "SELECT $2 AS second")
	// Keep legacy behavior: parameter extraction scans the full input.
	require.Len(t, result.Parameters, 2)
	assert.Equal(t, "$1", result.Parameters[0].Raw)
	assert.Equal(t, "$2", result.Parameters[1].Raw)
}

func TestParseSQLMultiStatementCreateTableFirstOnly(t *testing.T) {
	result, err := ParseSQL(multiCreateTableSQL)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, QueryCommandDDL, result.Command)
	require.Len(t, result.DDLActions, 1)
	assert.Equal(t, DDLCreateTable, result.DDLActions[0].Type)
	assert.Equal(t, "api_key", strings.ToLower(result.DDLActions[0].ObjectName))
	assert.Contains(t, result.RawSQL, "CREATE TABLE public.sometable")
}

func TestParseSQLAllReturnsAllStatementsInOrder(t *testing.T) {
	sql := "SET client_min_messages = warning; SELECT $1 AS v; CREATE TABLE public.t(id integer);"

	batch, err := ParseSQLAll(sql)
	require.NoError(t, err)
	require.NotNil(t, batch)

	assert.Equal(t, 3, batch.TotalStatements)
	assert.Equal(t, 3, batch.ParsedStatements)
	require.Len(t, batch.Queries, 3)
	require.Len(t, batch.Warnings, 1)
	assert.Equal(t, ParseWarningCodeFirstStatementOnly, batch.Warnings[0].Code)
	assert.Contains(t, batch.Warnings[0].Message, "additional statement(s) detected")

	assert.Equal(t, QueryCommandUnknown, batch.Queries[0].Command)
	assert.Equal(t, QueryCommandSelect, batch.Queries[1].Command)
	assert.Equal(t, QueryCommandDDL, batch.Queries[2].Command)
	assert.Contains(t, batch.Queries[1].RawSQL, "SELECT $1 AS v")
	assert.NotContains(t, batch.Queries[1].RawSQL, "CREATE TABLE")
	require.Len(t, batch.Queries[1].Parameters, 1)
	assert.Equal(t, "$1", batch.Queries[1].Parameters[0].Raw)
}

func TestParseSQLAllMultiStatementCreateTableBothStatements(t *testing.T) {
	batch, err := ParseSQLAll(multiCreateTableSQL)
	require.NoError(t, err)
	require.NotNil(t, batch)

	assert.Equal(t, 2, batch.TotalStatements)
	assert.Equal(t, 2, batch.ParsedStatements)
	require.Len(t, batch.Warnings, 1)
	assert.Equal(t, ParseWarningCodeFirstStatementOnly, batch.Warnings[0].Code)

	require.Len(t, batch.Queries, 2)
	assert.Equal(t, QueryCommandDDL, batch.Queries[0].Command)
	assert.Equal(t, QueryCommandDDL, batch.Queries[1].Command)

	require.Len(t, batch.Queries[0].DDLActions, 1)
	assert.Equal(t, "api_key", strings.ToLower(batch.Queries[0].DDLActions[0].ObjectName))
	require.Len(t, batch.Queries[1].DDLActions, 1)
	assert.Equal(t, "sometable", strings.ToLower(batch.Queries[1].DDLActions[0].ObjectName))

	assert.NotContains(t, batch.Queries[0].RawSQL, "sometable")
	assert.Contains(t, batch.Queries[1].RawSQL, "sometable")
}

func TestParseSQLAllSingleStatementHasNoWarnings(t *testing.T) {
	batch, err := ParseSQLAll("SELECT 1")
	require.NoError(t, err)
	require.NotNil(t, batch)
	assert.Equal(t, 1, batch.TotalStatements)
	assert.Equal(t, 1, batch.ParsedStatements)
	require.Len(t, batch.Queries, 1)
	assert.Empty(t, batch.Warnings)
}

func TestParseSQLAllEmptyInputReturnsErrNoStatements(t *testing.T) {
	batch, err := ParseSQLAll("  \n\t ")
	require.Error(t, err)
	assert.Nil(t, batch)
	assert.ErrorIs(t, err, ErrNoStatements)
}

func TestParseSQLAllInvalidSQLReturnsParseErrors(t *testing.T) {
	batch, err := ParseSQLAll("SELECT FROM")
	require.Error(t, err)
	assert.Nil(t, batch)

	var parseErr *ParseErrors
	assert.True(t, errors.As(err, &parseErr))
}

func TestParseSQLStrictRequiresExactlyOneStatement(t *testing.T) {
	t.Run("single statement succeeds", func(t *testing.T) {
		result, err := ParseSQLStrict("SELECT 1")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, QueryCommandSelect, result.Command)
	})

	t.Run("multiple statements fail", func(t *testing.T) {
		result, err := ParseSQLStrict("SELECT 1; SELECT 2")
		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorIs(t, err, ErrMultipleStatements)

		var strictErr *MultipleStatementsError
		require.True(t, errors.As(err, &strictErr))
		assert.Equal(t, 2, strictErr.StatementCount)
	})

	t.Run("no statement fails", func(t *testing.T) {
		result, err := ParseSQLStrict("   \n\t ")
		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorIs(t, err, ErrNoStatements)
	})
}

func TestParseSQLAllComplexMixedStatementsKeepPerStatementStructure(t *testing.T) {
	sql := `
SELECT u.id, COUNT(o.id) AS order_count
FROM users u
LEFT JOIN orders o ON o.user_id = u.id
WHERE u.active = true AND o.created_at > $1
GROUP BY u.id
HAVING COUNT(o.id) > 1
ORDER BY order_count DESC
LIMIT 10;
UPDATE users
SET status = 'active'
WHERE id = $2
RETURNING id;
DELETE FROM sessions WHERE expires_at < NOW();`

	batch, err := ParseSQLAll(sql)
	require.NoError(t, err)
	require.NotNil(t, batch)

	assert.Equal(t, 3, batch.TotalStatements)
	assert.Equal(t, 3, batch.ParsedStatements)
	require.Len(t, batch.Queries, 3)

	// 1) Complex SELECT statement
	selectQ := batch.Queries[0]
	assert.Equal(t, QueryCommandSelect, selectQ.Command)
	assert.GreaterOrEqual(t, len(selectQ.Tables), 2)
	assert.NotEmpty(t, selectQ.JoinConditions)
	assert.NotEmpty(t, selectQ.Where)
	assert.NotEmpty(t, selectQ.GroupBy)
	assert.NotEmpty(t, selectQ.Having)
	assert.NotEmpty(t, selectQ.OrderBy)
	assert.NotNil(t, selectQ.Limit)
	require.Len(t, selectQ.Parameters, 1)
	assert.Equal(t, "$1", selectQ.Parameters[0].Raw)

	// 2) UPDATE statement
	updateQ := batch.Queries[1]
	assert.Equal(t, QueryCommandUpdate, updateQ.Command)
	assert.NotEmpty(t, updateQ.SetClauses)
	assert.NotEmpty(t, updateQ.Where)
	assert.NotEmpty(t, updateQ.Returning)
	require.Len(t, updateQ.Parameters, 1)
	assert.Equal(t, "$2", updateQ.Parameters[0].Raw)

	// 3) DELETE statement
	deleteQ := batch.Queries[2]
	assert.Equal(t, QueryCommandDelete, deleteQ.Command)
	assert.NotEmpty(t, deleteQ.Where)
	assert.Empty(t, deleteQ.Parameters)
}
