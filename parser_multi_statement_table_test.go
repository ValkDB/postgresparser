package postgresparser

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSQLAllTable(t *testing.T) {
	tests := []struct {
		name             string
		sql              string
		wantErrIs        error
		wantParseErrType bool
		wantTotal        int
		wantParsed       int
		wantCommands     []QueryCommand
		wantWarnings     int
		wantWarningCode  string
		assertBatch      func(t *testing.T, batch *ParseBatchResult)
	}{
		{
			name:         "single statement select",
			sql:          "SELECT 1",
			wantTotal:    1,
			wantParsed:   1,
			wantCommands: []QueryCommand{QueryCommandSelect},
			wantWarnings: 0,
		},
		{
			name:            "mixed multi statement with warning",
			sql:             "SET client_min_messages = warning; SELECT $1 AS v; CREATE TABLE public.t(id integer);",
			wantTotal:       3,
			wantParsed:      3,
			wantCommands:    []QueryCommand{QueryCommandUnknown, QueryCommandSelect, QueryCommandDDL},
			wantWarnings:    1,
			wantWarningCode: ParseWarningCodeFirstStatementOnly,
		},
		{
			name:            "two create table statements",
			sql:             multiCreateTableSQL,
			wantTotal:       2,
			wantParsed:      2,
			wantCommands:    []QueryCommand{QueryCommandDDL, QueryCommandDDL},
			wantWarnings:    1,
			wantWarningCode: ParseWarningCodeFirstStatementOnly,
			assertBatch: func(t *testing.T, batch *ParseBatchResult) {
				t.Helper()
				require.Len(t, batch.Queries[0].DDLActions, 1)
				require.Len(t, batch.Queries[1].DDLActions, 1)
				assert.Equal(t, "api_key", batch.Queries[0].DDLActions[0].ObjectName)
				assert.Equal(t, "sometable", batch.Queries[1].DDLActions[0].ObjectName)
			},
		},
		{
			name:      "empty input",
			sql:       " \n\t ",
			wantErrIs: ErrNoStatements,
		},
		{
			name:             "invalid sql",
			sql:              "SELECT FROM",
			wantParseErrType: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batch, err := ParseSQLAll(tc.sql)

			if tc.wantErrIs != nil || tc.wantParseErrType {
				require.Error(t, err)
				assert.Nil(t, batch)
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
			require.NotNil(t, batch)
			assert.Equal(t, tc.wantTotal, batch.TotalStatements)
			assert.Equal(t, tc.wantParsed, batch.ParsedStatements)
			require.Len(t, batch.Queries, len(tc.wantCommands))
			assert.Len(t, batch.Warnings, tc.wantWarnings)
			if tc.wantWarningCode != "" {
				require.NotEmpty(t, batch.Warnings)
				assert.Equal(t, tc.wantWarningCode, batch.Warnings[0].Code)
			}
			for i := range tc.wantCommands {
				assert.Equal(t, tc.wantCommands[i], batch.Queries[i].Command)
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
