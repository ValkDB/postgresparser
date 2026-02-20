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

func TestAnalyzeSQLWithOptions_Table(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		opts         postgresparser.ParseOptions
		wantComments []string
		wantDDLType  string
		wantComment  string
	}{
		{
			name: "field comments enabled",
			sql: `CREATE TABLE public.users (
    -- comment line
    name text
);`,
			opts:         postgresparser.ParseOptions{IncludeCreateTableFieldComments: true},
			wantComments: []string{"comment line"},
			wantDDLType:  "CREATE_TABLE",
		},
		{
			name: "field comments disabled by default",
			sql: `CREATE TABLE public.users (
    -- comment line
    name text
);`,
			opts:         postgresparser.ParseOptions{},
			wantComments: []string{},
			wantDDLType:  "CREATE_TABLE",
		},
		{
			name:        "comment on index always on",
			sql:         `COMMENT ON INDEX public.idx_bookings_dates IS 'Composite index for efficient date range queries on bookings';`,
			opts:        postgresparser.ParseOptions{},
			wantDDLType: "COMMENT",
			wantComment: "Composite index for efficient date range queries on bookings",
		},
		{
			name:        "comment on table always on with flag enabled",
			sql:         `COMMENT ON TABLE public.users IS 'Stores user account information';`,
			opts:        postgresparser.ParseOptions{IncludeCreateTableFieldComments: true},
			wantDDLType: "COMMENT",
			wantComment: "Stores user account information",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := AnalyzeSQLWithOptions(tc.sql, tc.opts)
			require.NoError(t, err)
			require.NotNil(t, res)
			require.Len(t, res.DDLActions, 1)
			act := res.DDLActions[0]
			assert.Equal(t, tc.wantDDLType, act.Type)
			if tc.wantDDLType == "CREATE_TABLE" {
				require.Len(t, act.ColumnDetails, 1)
				if len(tc.wantComments) == 0 {
					assert.Empty(t, act.ColumnDetails[0].Comment)
				} else {
					assert.Equal(t, tc.wantComments, act.ColumnDetails[0].Comment)
				}
			}
			if tc.wantDDLType == "COMMENT" {
				assert.Equal(t, tc.wantComment, act.Comment)
			}
		})
	}
}

func TestAnalyzeSQLAllWithOptions_Table(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		opts        postgresparser.ParseOptions
		assertBatch func(t *testing.T, batch *SQLAnalysisBatchResult)
	}{
		{
			name: "field comments enabled for all statements",
			sql: `CREATE TABLE users (
    -- first
    name text
);
CREATE TABLE users2 (
    -- second
    email text
);`,
			opts: postgresparser.ParseOptions{IncludeCreateTableFieldComments: true},
			assertBatch: func(t *testing.T, batch *SQLAnalysisBatchResult) {
				t.Helper()
				require.Len(t, batch.Statements, 2)
				require.NotNil(t, batch.Statements[0].Query)
				require.NotNil(t, batch.Statements[1].Query)
				assert.Equal(t, []string{"first"}, batch.Statements[0].Query.DDLActions[0].ColumnDetails[0].Comment)
				assert.Equal(t, []string{"second"}, batch.Statements[1].Query.DDLActions[0].ColumnDetails[0].Comment)
			},
		},
		{
			name: "field comments disabled by default for all statements",
			sql: `CREATE TABLE users (
    -- first
    name text
);
CREATE TABLE users2 (
    -- second
    email text
);`,
			opts: postgresparser.ParseOptions{},
			assertBatch: func(t *testing.T, batch *SQLAnalysisBatchResult) {
				t.Helper()
				require.Len(t, batch.Statements, 2)
				require.NotNil(t, batch.Statements[0].Query)
				require.NotNil(t, batch.Statements[1].Query)
				assert.Empty(t, batch.Statements[0].Query.DDLActions[0].ColumnDetails[0].Comment)
				assert.Empty(t, batch.Statements[1].Query.DDLActions[0].ColumnDetails[0].Comment)
			},
		},
		{
			name: "comment statements always on in batch",
			sql: `COMMENT ON TABLE public.users IS 'Stores user account information';
COMMENT ON COLUMN public.users.email IS 'User email address, must be unique';`,
			opts: postgresparser.ParseOptions{},
			assertBatch: func(t *testing.T, batch *SQLAnalysisBatchResult) {
				t.Helper()
				require.Len(t, batch.Statements, 2)
				require.NotNil(t, batch.Statements[0].Query)
				require.NotNil(t, batch.Statements[1].Query)
				assert.Equal(t, "COMMENT", batch.Statements[0].Query.DDLActions[0].Type)
				assert.Equal(t, "COMMENT", batch.Statements[1].Query.DDLActions[0].Type)
				assert.Equal(t, "Stores user account information", batch.Statements[0].Query.DDLActions[0].Comment)
				assert.Equal(t, "User email address, must be unique", batch.Statements[1].Query.DDLActions[0].Comment)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batch, err := AnalyzeSQLAllWithOptions(tc.sql, tc.opts)
			require.NoError(t, err)
			require.NotNil(t, batch)
			if tc.assertBatch != nil {
				tc.assertBatch(t, batch)
			}
		})
	}
}

func TestAnalyzeSQLStrictWithOptions_Table(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		opts        postgresparser.ParseOptions
		wantErrIs   error
		assertQuery func(t *testing.T, q *SQLAnalysis)
	}{
		{
			name: "strict field comments enabled",
			sql: `CREATE TABLE users (
    -- strict analysis
    name text
);`,
			opts: postgresparser.ParseOptions{IncludeCreateTableFieldComments: true},
			assertQuery: func(t *testing.T, q *SQLAnalysis) {
				t.Helper()
				require.Len(t, q.DDLActions, 1)
				require.Len(t, q.DDLActions[0].ColumnDetails, 1)
				assert.Equal(t, []string{"strict analysis"}, q.DDLActions[0].ColumnDetails[0].Comment)
			},
		},
		{
			name: "strict comment always on",
			sql:  `COMMENT ON INDEX public.idx_bookings_dates IS 'Composite index for efficient date range queries on bookings';`,
			opts: postgresparser.ParseOptions{},
			assertQuery: func(t *testing.T, q *SQLAnalysis) {
				t.Helper()
				require.Len(t, q.DDLActions, 1)
				assert.Equal(t, "COMMENT", q.DDLActions[0].Type)
				assert.Equal(t, "Composite index for efficient date range queries on bookings", q.DDLActions[0].Comment)
			},
		},
		{
			name:      "strict rejects multi statement",
			sql:       "SELECT 1; SELECT 2;",
			opts:      postgresparser.ParseOptions{IncludeCreateTableFieldComments: true},
			wantErrIs: postgresparser.ErrMultipleStatements,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := AnalyzeSQLStrictWithOptions(tc.sql, tc.opts)
			if tc.wantErrIs != nil {
				require.Error(t, err)
				assert.Nil(t, res)
				assert.ErrorIs(t, err, tc.wantErrIs)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, res)
			if tc.assertQuery != nil {
				tc.assertQuery(t, res)
			}
		})
	}
}
