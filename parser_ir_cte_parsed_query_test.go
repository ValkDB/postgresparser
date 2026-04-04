package postgresparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIR_CTEParsedQuery_Select(t *testing.T) {
	sql := `WITH active_users AS (
    SELECT users.id, users.name FROM users WHERE users.active = $1
)
SELECT active_users.id, active_users.name
FROM active_users
WHERE active_users.id = $2`

	ir := parseAssertNoError(t, sql)
	require.Len(t, ir.CTEs, 1)

	cte := ir.CTEs[0]
	require.NotNil(t, cte.ParsedQuery)
	assert.Equal(t, QueryCommandSelect, cte.ParsedQuery.Command)
	assert.Equal(t, cte.Query, cte.ParsedQuery.RawSQL)

	require.Len(t, cte.ParsedQuery.Columns, 2)
	assert.Equal(t, "users.id", cte.ParsedQuery.Columns[0].Expression)
	assert.Equal(t, "users.name", cte.ParsedQuery.Columns[1].Expression)
	assert.True(t, containsTable(cte.ParsedQuery.Tables, "users"))

	require.Len(t, cte.ParsedQuery.Parameters, 1)
	assert.Equal(t, "$1", cte.ParsedQuery.Parameters[0].Raw)

	var foundFilter bool
	for _, usage := range cte.ParsedQuery.ColumnUsage {
		if usage.UsageType == ColumnUsageTypeFilter && usage.Column == "active" {
			foundFilter = true
			break
		}
	}
	assert.True(t, foundFilter, "expected CTE parsed query to retain filter column usage")
}

func TestIR_CTEParsedQuery_Update(t *testing.T) {
	sql := `WITH updated_users AS (
    UPDATE users SET active = true WHERE id = $1 RETURNING id
)
SELECT * FROM updated_users`

	ir := parseAssertNoError(t, sql)
	require.Len(t, ir.CTEs, 1)

	cte := ir.CTEs[0]
	require.NotNil(t, cte.ParsedQuery)
	assert.Equal(t, QueryCommandUpdate, cte.ParsedQuery.Command)
	assert.True(t, containsTable(cte.ParsedQuery.Tables, "users"))
	assert.NotEmpty(t, cte.ParsedQuery.SetClauses)
	assert.NotEmpty(t, cte.ParsedQuery.Returning)
	require.Len(t, cte.ParsedQuery.Parameters, 1)
	assert.Equal(t, "$1", cte.ParsedQuery.Parameters[0].Raw)
}
