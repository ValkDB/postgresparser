package analysis

import (
	"errors"
	"testing"

	"github.com/valkdb/postgresparser"
)

const multiCreateTableSQLAnalysis = `
CREATE TABLE public.api_key (
    id integer NOT NULL
);
CREATE TABLE public.sometable (
    id integer NOT NULL
);`

func TestAnalyzeSQLAllTable(t *testing.T) {
	tests := []struct {
		name             string
		sql              string
		wantErrIs        error
		wantParseErrType bool
		wantTotal        int
		wantParsed       int
		wantCommands     []SQLCommand
		wantWarnings     int
		wantWarningCode  string
		assertBatch      func(t *testing.T, batch *SQLAnalysisBatchResult)
	}{
		{
			name:         "single statement select",
			sql:          "SELECT 1",
			wantTotal:    1,
			wantParsed:   1,
			wantCommands: []SQLCommand{SQLCommandSelect},
			wantWarnings: 0,
		},
		{
			name:            "mixed multi statement with warning",
			sql:             "SET client_min_messages = warning; SELECT $1 AS value;",
			wantTotal:       2,
			wantParsed:      2,
			wantCommands:    []SQLCommand{SQLCommandUnknown, SQLCommandSelect},
			wantWarnings:    1,
			wantWarningCode: postgresparser.ParseWarningCodeFirstStatementOnly,
		},
		{
			name:            "two create table statements",
			sql:             multiCreateTableSQLAnalysis,
			wantTotal:       2,
			wantParsed:      2,
			wantCommands:    []SQLCommand{SQLCommandDDL, SQLCommandDDL},
			wantWarnings:    1,
			wantWarningCode: postgresparser.ParseWarningCodeFirstStatementOnly,
			assertBatch: func(t *testing.T, batch *SQLAnalysisBatchResult) {
				t.Helper()
				if len(batch.Queries[0].DDLActions) != 1 || batch.Queries[0].DDLActions[0].ObjectName != "api_key" {
					t.Fatalf("expected first DDL action for api_key, got %+v", batch.Queries[0].DDLActions)
				}
				if len(batch.Queries[1].DDLActions) != 1 || batch.Queries[1].DDLActions[0].ObjectName != "sometable" {
					t.Fatalf("expected second DDL action for sometable, got %+v", batch.Queries[1].DDLActions)
				}
			},
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batch, err := AnalyzeSQLAll(tc.sql)

			if tc.wantErrIs != nil || tc.wantParseErrType {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if batch != nil {
					t.Fatalf("expected nil batch on error")
				}
				if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
				}
				if tc.wantParseErrType {
					var parseErr *postgresparser.ParseErrors
					if !errors.As(err, &parseErr) {
						t.Fatalf("expected ParseErrors type, got %T", err)
					}
				}
				return
			}

			if err != nil {
				t.Fatalf("AnalyzeSQLAll failed: %v", err)
			}
			if batch == nil {
				t.Fatalf("expected non-nil batch result")
			}
			if batch.TotalStatements != tc.wantTotal || batch.ParsedStatements != tc.wantParsed {
				t.Fatalf("unexpected statement counters: %+v", batch)
			}
			if len(batch.Queries) != len(tc.wantCommands) {
				t.Fatalf("expected %d analysis queries, got %d", len(tc.wantCommands), len(batch.Queries))
			}
			if len(batch.Warnings) != tc.wantWarnings {
				t.Fatalf("expected %d warnings, got %+v", tc.wantWarnings, batch.Warnings)
			}
			if tc.wantWarningCode != "" {
				if len(batch.Warnings) == 0 || batch.Warnings[0].Code != tc.wantWarningCode {
					t.Fatalf("expected warning code %s, got %+v", tc.wantWarningCode, batch.Warnings)
				}
			}
			for i := range tc.wantCommands {
				if batch.Queries[i].Command != tc.wantCommands[i] {
					t.Fatalf("unexpected command at index %d: want %s got %s", i, tc.wantCommands[i], batch.Queries[i].Command)
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
	}{
		{
			name:        "single select",
			sql:         "SELECT 1",
			wantCommand: SQLCommandSelect,
		},
		{
			name:        "single ddl",
			sql:         "CREATE TABLE users(id integer)",
			wantCommand: SQLCommandDDL,
		},
		{
			name:      "multi statement",
			sql:       "SELECT 1; SELECT 2",
			wantErrIs: postgresparser.ErrMultipleStatements,
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := AnalyzeSQLStrict(tc.sql)

			if tc.wantErrIs != nil || tc.wantParseErrType {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if res != nil {
					t.Fatalf("expected nil result on error")
				}
				if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
				}
				if tc.wantParseErrType {
					var parseErr *postgresparser.ParseErrors
					if !errors.As(err, &parseErr) {
						t.Fatalf("expected ParseErrors type, got %T", err)
					}
				}
				return
			}

			if err != nil {
				t.Fatalf("AnalyzeSQLStrict failed: %v", err)
			}
			if res == nil {
				t.Fatalf("expected non-nil result")
			}
			if res.Command != tc.wantCommand {
				t.Fatalf("expected command %s, got %s", tc.wantCommand, res.Command)
			}
		})
	}
}
