package analysis

import (
	"errors"
	"testing"

	"github.com/valkdb/postgresparser"
)

func TestAnalyzeSQLAllReturnsAllStatementsInOrder(t *testing.T) {
	sql := "SET client_min_messages = warning; SELECT $1 AS value;"

	batch, err := AnalyzeSQLAll(sql)
	if err != nil {
		t.Fatalf("AnalyzeSQLAll failed: %v", err)
	}
	if batch == nil {
		t.Fatalf("expected non-nil batch result")
	}
	if batch.TotalStatements != 2 || batch.ParsedStatements != 2 {
		t.Fatalf("unexpected statement counters: %+v", batch)
	}
	if len(batch.Warnings) != 1 || batch.Warnings[0].Code != postgresparser.ParseWarningCodeFirstStatementOnly {
		t.Fatalf("expected FIRST_STATEMENT_ONLY warning, got %+v", batch.Warnings)
	}
	if len(batch.Queries) != 2 {
		t.Fatalf("expected 2 analysis queries, got %d", len(batch.Queries))
	}
	if batch.Queries[0].Command != SQLCommandUnknown {
		t.Fatalf("expected UNKNOWN for first statement, got %s", batch.Queries[0].Command)
	}
	if batch.Queries[1].Command != SQLCommandSelect {
		t.Fatalf("expected SELECT for second statement, got %s", batch.Queries[1].Command)
	}
	if len(batch.Queries[1].Parameters) != 1 || batch.Queries[1].Parameters[0].Raw != "$1" {
		t.Fatalf("unexpected parameters for second statement: %+v", batch.Queries[1].Parameters)
	}
}

func TestAnalyzeSQLAllMultiStatementCreateTableBothStatements(t *testing.T) {
	sql := `
CREATE TABLE public.api_key (
    id integer NOT NULL
);
CREATE TABLE public.sometable (
    id integer NOT NULL
);`

	batch, err := AnalyzeSQLAll(sql)
	if err != nil {
		t.Fatalf("AnalyzeSQLAll failed: %v", err)
	}
	if batch == nil {
		t.Fatalf("expected non-nil batch result")
	}
	if batch.TotalStatements != 2 || batch.ParsedStatements != 2 {
		t.Fatalf("unexpected statement counters: %+v", batch)
	}
	if len(batch.Warnings) != 1 || batch.Warnings[0].Code != postgresparser.ParseWarningCodeFirstStatementOnly {
		t.Fatalf("expected FIRST_STATEMENT_ONLY warning, got %+v", batch.Warnings)
	}
	if len(batch.Queries) != 2 {
		t.Fatalf("expected 2 analysis queries, got %d", len(batch.Queries))
	}
	if batch.Queries[0].Command != SQLCommandDDL || batch.Queries[1].Command != SQLCommandDDL {
		t.Fatalf("expected DDL commands, got %+v and %+v", batch.Queries[0].Command, batch.Queries[1].Command)
	}
	if len(batch.Queries[0].DDLActions) != 1 || batch.Queries[0].DDLActions[0].ObjectName != "api_key" {
		t.Fatalf("expected first DDL action for api_key, got %+v", batch.Queries[0].DDLActions)
	}
	if len(batch.Queries[1].DDLActions) != 1 || batch.Queries[1].DDLActions[0].ObjectName != "sometable" {
		t.Fatalf("expected second DDL action for sometable, got %+v", batch.Queries[1].DDLActions)
	}
}

func TestAnalyzeSQLMultiStatementCreateTableFirstOnly(t *testing.T) {
	sql := `
CREATE TABLE public.api_key (
    id integer NOT NULL
);
CREATE TABLE public.sometable (
    id integer NOT NULL
);`

	res, err := AnalyzeSQL(sql)
	if err != nil {
		t.Fatalf("AnalyzeSQL failed: %v", err)
	}
	if res == nil {
		t.Fatalf("expected non-nil analysis result")
	}
	if res.Command != SQLCommandDDL {
		t.Fatalf("expected DDL command, got %s", res.Command)
	}
	if len(res.DDLActions) != 1 || res.DDLActions[0].ObjectName != "api_key" {
		t.Fatalf("expected first CREATE TABLE action only, got %+v", res.DDLActions)
	}
}

func TestAnalyzeSQLStrictRequiresExactlyOneStatement(t *testing.T) {
	t.Run("single statement succeeds", func(t *testing.T) {
		res, err := AnalyzeSQLStrict("SELECT 1")
		if err != nil {
			t.Fatalf("AnalyzeSQLStrict failed: %v", err)
		}
		if res == nil || res.Command != SQLCommandSelect {
			t.Fatalf("unexpected strict single-statement result: %+v", res)
		}
	})

	t.Run("multiple statements fail", func(t *testing.T) {
		res, err := AnalyzeSQLStrict("SELECT 1; SELECT 2")
		if err == nil {
			t.Fatalf("expected strict error, got result: %+v", res)
		}
		if res != nil {
			t.Fatalf("expected nil strict result")
		}
		if !errors.Is(err, postgresparser.ErrMultipleStatements) {
			t.Fatalf("expected ErrMultipleStatements, got %v", err)
		}
	})
}

func TestAnalyzeSQLAllComplexMixedStatementsKeepPerStatementStructure(t *testing.T) {
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

	batch, err := AnalyzeSQLAll(sql)
	if err != nil {
		t.Fatalf("AnalyzeSQLAll failed: %v", err)
	}
	if batch == nil {
		t.Fatalf("expected non-nil batch result")
	}
	if batch.TotalStatements != 3 || batch.ParsedStatements != 3 {
		t.Fatalf("unexpected statement counters: %+v", batch)
	}
	if len(batch.Warnings) != 1 || batch.Warnings[0].Code != postgresparser.ParseWarningCodeFirstStatementOnly {
		t.Fatalf("expected FIRST_STATEMENT_ONLY warning, got %+v", batch.Warnings)
	}
	if len(batch.Queries) != 3 {
		t.Fatalf("expected 3 analysis queries, got %d", len(batch.Queries))
	}

	// 1) Complex SELECT statement
	selectQ := batch.Queries[0]
	if selectQ.Command != SQLCommandSelect {
		t.Fatalf("expected SELECT command, got %s", selectQ.Command)
	}
	if len(selectQ.Tables) < 2 {
		t.Fatalf("expected at least 2 tables in SELECT, got %+v", selectQ.Tables)
	}
	if len(selectQ.JoinClauses) == 0 || len(selectQ.Where) == 0 || len(selectQ.GroupBy) == 0 {
		t.Fatalf("expected join/where/group metadata in SELECT: %+v", selectQ)
	}
	if len(selectQ.Having) == 0 || len(selectQ.OrderBy) == 0 || selectQ.Limit == nil {
		t.Fatalf("expected having/order/limit metadata in SELECT: %+v", selectQ)
	}
	if len(selectQ.Parameters) != 1 || selectQ.Parameters[0].Raw != "$1" {
		t.Fatalf("unexpected SELECT parameters: %+v", selectQ.Parameters)
	}

	// 2) UPDATE statement
	updateQ := batch.Queries[1]
	if updateQ.Command != SQLCommandUpdate {
		t.Fatalf("expected UPDATE command, got %s", updateQ.Command)
	}
	if len(updateQ.SetClauses) == 0 || len(updateQ.Where) == 0 || len(updateQ.Returning) == 0 {
		t.Fatalf("expected update metadata in UPDATE: %+v", updateQ)
	}
	if len(updateQ.Parameters) != 1 || updateQ.Parameters[0].Raw != "$2" {
		t.Fatalf("unexpected UPDATE parameters: %+v", updateQ.Parameters)
	}

	// 3) DELETE statement
	deleteQ := batch.Queries[2]
	if deleteQ.Command != SQLCommandDelete {
		t.Fatalf("expected DELETE command, got %s", deleteQ.Command)
	}
	if len(deleteQ.Where) == 0 {
		t.Fatalf("expected WHERE metadata in DELETE: %+v", deleteQ)
	}
	if len(deleteQ.Parameters) != 0 {
		t.Fatalf("expected no parameters in DELETE, got %+v", deleteQ.Parameters)
	}
}
