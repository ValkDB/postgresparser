package analysis

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/valkdb/postgresparser"
)

const multiCreateTableSQLAnalysis = `
CREATE TABLE public.api_key (
    id integer NOT NULL
);
CREATE TABLE public.sometable (
    id integer NOT NULL
);`

func TestAnalyzeSQLTable(t *testing.T) {
	tests := []struct {
		name             string
		sql              string
		wantErrIs        error
		wantParseErrType bool
		wantCommand      SQLCommand
		assertResult     func(t *testing.T, res *SQLAnalysis)
	}{
		{
			name:        "single statement",
			sql:         "SELECT 1",
			wantCommand: SQLCommandSelect,
		},
		{
			name:        "trailing semicolon",
			sql:         "SELECT 1;",
			wantCommand: SQLCommandSelect,
		},
		{
			name:        "legacy first statement behavior",
			sql:         "SELECT $1 AS first; SELECT $2 AS second;",
			wantCommand: SQLCommandSelect,
			assertResult: func(t *testing.T, res *SQLAnalysis) {
				t.Helper()
				assert.Contains(t, res.RawSQL, "SELECT $2 AS second")
				require.Len(t, res.Parameters, 2)
			},
		},
		{
			name:        "legacy create table first statement behavior",
			sql:         multiCreateTableSQLAnalysis,
			wantCommand: SQLCommandDDL,
			assertResult: func(t *testing.T, res *SQLAnalysis) {
				t.Helper()
				require.Len(t, res.DDLActions, 1)
				assert.Equal(t, "CREATE_TABLE", res.DDLActions[0].Type)
				assert.Equal(t, "api_key", res.DDLActions[0].ObjectName)
				assert.Contains(t, res.RawSQL, "CREATE TABLE public.sometable")
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
			res, err := AnalyzeSQL(tc.sql)

			if tc.wantErrIs != nil || tc.wantParseErrType {
				require.Error(t, err)
				assert.Nil(t, res)
				if tc.wantErrIs != nil {
					assert.ErrorIs(t, err, tc.wantErrIs)
				}
				if tc.wantParseErrType {
					var parseErr *postgresparser.ParseErrors
					assert.True(t, errors.As(err, &parseErr))
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, tc.wantCommand, res.Command)
			if tc.assertResult != nil {
				tc.assertResult(t, res)
			}
		})
	}
}

func TestAnalyzeSQLAllTable(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		wantErrIs     error
		wantStmtCount int
		wantFailed    bool
		wantCommands  []SQLCommand
		wantWarnCodes [][]SQLParseWarningCode
		assertBatch   func(t *testing.T, batch *SQLAnalysisBatchResult)
	}{
		{
			name:          "single statement select",
			sql:           "SELECT 1",
			wantStmtCount: 1,
			wantFailed:    false,
			wantCommands:  []SQLCommand{SQLCommandSelect},
		},
		{
			name:          "trailing semicolon",
			sql:           "SELECT 1;",
			wantStmtCount: 1,
			wantFailed:    false,
			wantCommands:  []SQLCommand{SQLCommandSelect},
		},
		{
			name:          "trailing double semicolon",
			sql:           "SELECT 1;;",
			wantStmtCount: 1,
			wantFailed:    false,
			wantCommands:  []SQLCommand{SQLCommandSelect},
		},
		{
			name:          "mixed multi statement",
			sql:           "SET client_min_messages = warning; SELECT $1 AS value;",
			wantStmtCount: 2,
			wantFailed:    false,
			wantCommands:  []SQLCommand{SQLCommandUnknown, SQLCommandSelect},
		},
		{
			name:          "two create table statements",
			sql:           multiCreateTableSQLAnalysis,
			wantStmtCount: 2,
			wantFailed:    false,
			wantCommands:  []SQLCommand{SQLCommandDDL, SQLCommandDDL},
			assertBatch: func(t *testing.T, batch *SQLAnalysisBatchResult) {
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
			wantCommands:  []SQLCommand{SQLCommandSelect},
			wantWarnCodes: [][]SQLParseWarningCode{
				{SQLParseWarningCodeSyntaxError},
			},
		},
		{
			name:          "invalid sql mid-batch warning is attached to statement index",
			sql:           "SELECT 1;\nSELECT FROM;\nSELECT 2;",
			wantStmtCount: 3,
			wantFailed:    true,
			wantCommands:  []SQLCommand{SQLCommandSelect, SQLCommandSelect, SQLCommandSelect},
			wantWarnCodes: [][]SQLParseWarningCode{
				nil,
				{SQLParseWarningCodeSyntaxError},
				nil,
			},
		},
		{
			name:          "complex mixed statements",
			sql:           `SELECT u.id, COUNT(o.id) AS order_count FROM users u LEFT JOIN orders o ON o.user_id = u.id WHERE u.active = true AND o.created_at > $1 GROUP BY u.id HAVING COUNT(o.id) > 1 ORDER BY order_count DESC LIMIT 10; UPDATE users SET status = 'active' WHERE id = $2 RETURNING id; DELETE FROM sessions WHERE expires_at < NOW();`,
			wantStmtCount: 3,
			wantFailed:    false,
			wantCommands:  []SQLCommand{SQLCommandSelect, SQLCommandUpdate, SQLCommandDelete},
			assertBatch: func(t *testing.T, batch *SQLAnalysisBatchResult) {
				t.Helper()
				selectQ := batch.Statements[0].Query
				assert.GreaterOrEqual(t, len(selectQ.Tables), 2)
				assert.NotEmpty(t, selectQ.JoinClauses)
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
			wantErrIs: postgresparser.ErrNoStatements,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batch, err := AnalyzeSQLAll(tc.sql)

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
					codes := make([]SQLParseWarningCode, 0, len(batch.Statements[i].Warnings))
					for _, w := range batch.Statements[i].Warnings {
						codes = append(codes, w.Code)
					}
					expected := tc.wantWarnCodes[i]
					if expected == nil {
						expected = []SQLParseWarningCode{}
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

func TestAnalyzeSQLStrictTable(t *testing.T) {
	tests := []struct {
		name             string
		sql              string
		wantErrIs        error
		wantParseErrType bool
		wantCommand      SQLCommand
		wantStmtCount    int
	}{
		{
			name:        "single select",
			sql:         "SELECT 1",
			wantCommand: SQLCommandSelect,
		},
		{
			name:        "single select trailing semicolon",
			sql:         "SELECT 1;",
			wantCommand: SQLCommandSelect,
		},
		{
			name:        "single ddl",
			sql:         "CREATE TABLE users(id integer)",
			wantCommand: SQLCommandDDL,
		},
		{
			name:          "multi statement",
			sql:           "SELECT 1; SELECT 2",
			wantErrIs:     postgresparser.ErrMultipleStatements,
			wantStmtCount: 2,
		},
		{
			name:      "empty input",
			sql:       " \n\t ",
			wantErrIs: postgresparser.ErrNoStatements,
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
			res, err := AnalyzeSQLStrict(tc.sql)

			if tc.wantErrIs != nil || tc.wantParseErrType {
				require.Error(t, err)
				assert.Nil(t, res)
				if tc.wantErrIs != nil {
					assert.ErrorIs(t, err, tc.wantErrIs)
				}
				if tc.wantStmtCount > 0 {
					var strictErr *postgresparser.MultipleStatementsError
					require.True(t, errors.As(err, &strictErr))
					assert.Equal(t, tc.wantStmtCount, strictErr.StatementCount)
				}
				if tc.wantParseErrType {
					var parseErr *postgresparser.ParseErrors
					assert.True(t, errors.As(err, &parseErr))
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, tc.wantCommand, res.Command)
		})
	}
}
