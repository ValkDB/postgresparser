package postgresparser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIR_PlaceholdersParserLevel(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		count int
		role  PlaceholderRole
		style string
	}{
		{"question placeholder", "SELECT * FROM t WHERE id = ?", 1, PlaceholderRoleWhereValue, "?"},
		{"dollar placeholder", "SELECT * FROM t WHERE id = $2", 1, PlaceholderRoleWhereValue, "$"},
		{"jsonb question operator", "SELECT * FROM t WHERE data ? 'key'", 0, PlaceholderRoleUnknown, ""},
		{"interval placeholder", "SELECT * FROM t WHERE created_at > now() - INTERVAL ?", 1, PlaceholderRoleIntervalOperand, "?"},
		{"extract placeholder", "SELECT extract(? FROM created_at) FROM t", 1, PlaceholderRoleFunctionArg, "?"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ParseSQL(tc.sql)
			require.NoError(t, err)
			require.Len(t, result.Placeholders, tc.count)
			if tc.count == 0 {
				return
			}
			require.Equal(t, tc.role, result.Placeholders[0].Role)
			require.Equal(t, tc.style, result.Placeholders[0].Style)
		})
	}
}

func TestIR_PlaceholdersMultiStatement(t *testing.T) {
	result, err := ParseSQLAll("SELECT * FROM a WHERE id = ?; SELECT * FROM b WHERE status = $1")
	require.NoError(t, err)
	require.Len(t, result.Statements, 2)
	require.Len(t, result.Statements[0].Query.Placeholders, 1)
	require.Len(t, result.Statements[1].Query.Placeholders, 1)
	require.Equal(t, "?", result.Statements[0].Query.Placeholders[0].Style)
	require.Equal(t, "$", result.Statements[1].Query.Placeholders[0].Style)
	require.Equal(t, PlaceholderRoleWhereValue, result.Statements[0].Query.Placeholders[0].Role)
	require.Equal(t, PlaceholderRoleWhereValue, result.Statements[1].Query.Placeholders[0].Role)
}

func TestIR_PlaceholdersPositionOffsets(t *testing.T) {
	sql := "SELECT 'é?' AS marker WHERE id = ?"
	result, err := ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, result.Placeholders, 1)

	start := strings.LastIndex(sql, "?")
	require.NotEqual(t, -1, start)
	require.Equal(t, start, result.Placeholders[0].Start)
	require.Equal(t, start+1, result.Placeholders[0].End)
	require.Equal(t, "?", sql[result.Placeholders[0].Start:result.Placeholders[0].End])
}
