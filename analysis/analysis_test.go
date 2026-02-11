package analysis

import (
	"strings"
	"testing"
)

// TestAnalyzeSQLSelect verifies that SELECT statements populate projection,
// table, CTE, and ORDER metadata in the DTO.
func TestAnalyzeSQLSelect(t *testing.T) {
	sql := `
WITH ranked AS (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY tenant ORDER BY created_at DESC) AS seq
    FROM orders
)
SELECT r.id, r.seq
FROM ranked r
WHERE r.seq <= 5
ORDER BY r.seq`

	res, err := AnalyzeSQL(sql)
	if err != nil {
		t.Fatalf("AnalyzeSQL failed: %v", err)
	}
	if res == nil {
		t.Fatalf("expected populated analysis result")
	}
	if res.Command != SQLCommandSelect {
		t.Fatalf("expected SELECT command, got %s", res.Command)
	}
	// Parser correctly extracts both base table (orders) and CTE (ranked)
	if len(res.Tables) != 2 {
		t.Fatalf("expected 2 tables (orders base + ranked CTE), got %d: %+v", len(res.Tables), res.Tables)
	}
	// Verify we have both tables
	hasOrders := false
	hasRanked := false
	for _, tbl := range res.Tables {
		if tbl.Name == "orders" && tbl.Type == SQLTableTypeBase {
			hasOrders = true
		}
		if tbl.Name == "ranked" && tbl.Type == SQLTableTypeCTE {
			hasRanked = true
		}
	}
	if !hasOrders || !hasRanked {
		t.Fatalf("expected both 'orders' (base) and 'ranked' (cte), got: %+v", res.Tables)
	}
	if len(res.Columns) != 2 {
		t.Fatalf("expected 2 projected columns, got %+v", res.Columns)
	}
	if len(res.CTEs) != 1 || !strings.Contains(res.CTEs[0].Query, "ROW_NUMBER()") {
		t.Fatalf("unexpected CTE metadata: %+v", res.CTEs)
	}
	if res.Limit != nil {
		t.Fatalf("did not expect LIMIT in analysis")
	}
	if len(res.OrderBy) != 1 || !strings.Contains(res.OrderBy[0].Expression, "seq") {
		t.Fatalf("unexpected ORDER BY metadata: %+v", res.OrderBy)
	}
	var filterCount, orderCount int
	for _, u := range res.ColumnUsage {
		switch u.UsageType {
		case SQLUsageTypeFilter:
			if u.Column == "seq" {
				filterCount++
			}
		case SQLUsageTypeOrder:
			if u.Column == "seq" {
				orderCount++
			}
		}
	}
	if filterCount == 0 {
		t.Fatalf("expected filter usage for seq %+v", res.ColumnUsage)
	}
	if orderCount == 0 {
		t.Fatalf("expected order usage for seq %+v", res.ColumnUsage)
	}
}

// TestAnalyzeSQLInsertUpsert ensures INSERT ... ON CONFLICT metadata is surfaced.
func TestAnalyzeSQLInsertUpsert(t *testing.T) {
	sql := `
INSERT INTO users (id, email, status)
VALUES (?, ?, ?)
ON CONFLICT (email)
DO UPDATE SET status = EXCLUDED.status
RETURNING id`

	res, err := AnalyzeSQL(sql)
	if err != nil {
		t.Fatalf("AnalyzeSQL failed: %v", err)
	}
	if res.Command != SQLCommandInsert {
		t.Fatalf("expected INSERT command, got %s", res.Command)
	}
	if len(res.InsertColumns) != 3 {
		t.Fatalf("expected 3 insert columns, got %+v", res.InsertColumns)
	}
	if res.Upsert == nil || res.Upsert.Action != "DO UPDATE" {
		t.Fatalf("expected upsert metadata, got %+v", res.Upsert)
	}
	if len(res.Upsert.SetClauses) == 0 {
		t.Fatalf("expected set clauses for upsert update, got %+v", res.Upsert.SetClauses)
	}
	if len(res.Returning) != 1 || res.Returning[0] != "id" {
		t.Fatalf("unexpected RETURNING metadata: %+v", res.Returning)
	}
	if len(res.Parameters) != 3 {
		t.Fatalf("expected 3 parameters for VALUES placeholders, got %+v", res.Parameters)
	}
	var setSeen, returningSeen bool
	for _, u := range res.ColumnUsage {
		switch u.UsageType {
		case SQLUsageTypeDMLSet:
			if u.Column == "status" {
				setSeen = true
			}
		case SQLUsageTypeReturning:
			if u.Column == "id" {
				returningSeen = true
			}
		}
	}
	if !setSeen {
		t.Fatalf("expected ColumnUsage for SET status, got %+v", res.ColumnUsage)
	}
	if !returningSeen {
		t.Fatalf("expected ColumnUsage for RETURNING id, got %+v", res.ColumnUsage)
	}
}

// TestAnalyzeSQLMultiJoinUsage verifies ColumnUsage distinguishes aliases in joins.
func TestAnalyzeSQLMultiJoinUsage(t *testing.T) {
	sql := `SELECT o.id, c.name, u.name
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN users u ON o.assigned_to = u.id
WHERE o.status = 'open'`

	res, err := AnalyzeSQL(sql)
	if err != nil {
		t.Fatalf("AnalyzeSQL failed: %v", err)
	}
	count := map[string]map[string]int{}
	for _, usage := range res.ColumnUsage {
		if usage.UsageType != SQLUsageTypeJoin {
			continue
		}
		alias := usage.TableAlias
		if alias == "" {
			alias = "(empty)"
		}
		if count[alias] == nil {
			count[alias] = map[string]int{}
		}
		count[alias][usage.Column]++
	}
	if count["c"]["id"] == 0 {
		t.Fatalf("expected join usage for customers.id, got %#v", count)
	}
	if count["u"]["id"] == 0 {
		t.Fatalf("expected join usage for users.id, got %#v", count)
	}
	if count["o"]["customer_id"] == 0 || count["o"]["assigned_to"] == 0 {
		t.Fatalf("expected join usage for order foreign keys, got %#v", count)
	}
	if count["o"]["id"] != 0 {
		t.Fatalf("unexpected join attribution for o.id, got %#v", count)
	}
}

// TestAnalyzeSQLSetClientMinMessages confirms the analysis layer handles SET
// with grammar-unfriendly log-level tokens gracefully.
func TestAnalyzeSQLSetClientMinMessages(t *testing.T) {
	res, err := AnalyzeSQL("SET client_min_messages = warning")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res == nil {
		t.Fatalf("expected non-nil result")
	}
	if res.Command != SQLCommandUnknown {
		t.Fatalf("expected UNKNOWN command, got %s", res.Command)
	}
}

// TestAnalyzeSQLSetLogLevelRecovery confirms analysis handles SET statements
// whose RHS log-level token requires utility parse-error recovery.
func TestAnalyzeSQLSetLogLevelRecovery(t *testing.T) {
	res, err := AnalyzeSQL("SET foo = warning")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res == nil {
		t.Fatalf("expected non-nil result")
	}
	if res.Command != SQLCommandUnknown {
		t.Fatalf("expected UNKNOWN command, got %s", res.Command)
	}
}

// TestAnalyzeSQLSetClientMinMessagesMalformed verifies malformed client_min_messages
// statements still return parse errors.
func TestAnalyzeSQLSetClientMinMessagesMalformed(t *testing.T) {
	result, err := AnalyzeSQL("SET client_min_messages = warning]")
	if err == nil {
		t.Fatalf("expected parse error, got result: %+v", result)
	}
	if result != nil {
		t.Fatalf("expected nil result on parse error")
	}
}

// TestAnalyzeSQLInvalidUtilityStatementsReturnError ensures malformed utility SQL
// is never silently accepted as UNKNOWN.
func TestAnalyzeSQLInvalidUtilityStatementsReturnError(t *testing.T) {
	tests := []string{
		"SET log_min_messages = warning]",
		"SET log_min_messages = warning; SELECT 1",
		"SHOW",
		"RESET ALL extra",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			result, err := AnalyzeSQL(sql)
			if err == nil {
				t.Fatalf("expected parse error, got result: %+v", result)
			}
			if result != nil {
				t.Fatalf("expected nil result on parse error")
			}
		})
	}
}

// TestAnalyzeSQLParseError verifies that invalid SQL returns an error.
func TestAnalyzeSQLParseError(t *testing.T) {
	result, err := AnalyzeSQL("SELECT * FROM (SELECT 1")
	if err == nil {
		t.Fatalf("expected error for invalid SQL, got result: %+v", result)
	}
	if result != nil {
		t.Fatalf("expected nil result on parse error")
	}
}
