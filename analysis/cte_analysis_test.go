package analysis

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeSQL_CTEAnalysis(t *testing.T) {
	sql := `WITH active_users AS (
    SELECT users.id, users.name FROM users WHERE users.active = $1
)
SELECT active_users.id, active_users.name
FROM active_users
WHERE active_users.id = $2`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.CTEs, 1)

	cte := result.CTEs[0]
	require.NotNil(t, cte.Analysis)
	assert.Equal(t, SQLCommandSelect, cte.Analysis.Command)
	assert.Equal(t, cte.Query, cte.Analysis.RawSQL)
	require.Len(t, cte.Analysis.Columns, 2)
	assert.Equal(t, "users.id", cte.Analysis.Columns[0].Expression)
	assert.Equal(t, "users.name", cte.Analysis.Columns[1].Expression)

	require.Len(t, cte.Analysis.Parameters, 1)
	assert.Equal(t, "$1", cte.Analysis.Parameters[0].Raw)

	var foundUsers bool
	for _, table := range cte.Analysis.Tables {
		if table.Name == "users" {
			foundUsers = true
			break
		}
	}
	assert.True(t, foundUsers, "expected nested CTE analysis to include users table")
}
