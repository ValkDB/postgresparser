package analysis

import (
	"testing"
)

// TestColumnsForTableUsesColumnUsage validates ColumnsForTable filters by alias and usage types.
func TestColumnsForTableUsesColumnUsage(t *testing.T) {
	sql := `SELECT a.id, a.name, b.id FROM users a JOIN orders b ON a.id = b.user_id WHERE a.status = ? ORDER BY a.created_at`
	an, err := AnalyzeSQL(sql)
	if err != nil {
		t.Fatalf("analysis failed: %v", err)
	}
	cols := ColumnsForTable(an, "a", SQLUsageTypeJoin, SQLUsageTypeFilter, SQLUsageTypeOrder, SQLUsageTypeProjection)
	expected := map[string]bool{"id": true, "name": true, "status": true, "created_at": true}
	if len(cols) != len(expected) {
		t.Fatalf("unexpected columns: %#v", cols)
	}
	for _, c := range cols {
		if !expected[c] {
			t.Fatalf("unexpected column %s in %#v", c, cols)
		}
		delete(expected, c)
	}
	if len(expected) != 0 {
		t.Fatalf("missing columns: %#v", expected)
	}
}

// TestUsageByRolesFilter verifies UsageByRoles returns only the requested usage type.
func TestUsageByRolesFilter(t *testing.T) {
	sql := `SELECT id FROM logs WHERE created_at > NOW() AND status = 'ok'`
	an, err := AnalyzeSQL(sql)
	if err != nil {
		t.Fatalf("analysis failed: %v", err)
	}
	usages := UsageByRoles(an, SQLUsageTypeFilter)
	if len(usages) == 0 {
		t.Fatalf("expected filter usages, got %#v", usages)
	}
	for _, u := range usages {
		if u.UsageType != SQLUsageTypeFilter {
			t.Fatalf("expected filter usage, got %+v", u)
		}
	}
}

// TestLimitValue confirms LimitValue extracts the numeric LIMIT from analysis.
func TestLimitValue(t *testing.T) {
	sql := `SELECT * FROM metrics LIMIT 10`
	an, err := AnalyzeSQL(sql)
	if err != nil {
		t.Fatalf("analysis failed: %v", err)
	}
	if v := LimitValue(an); v != 10 {
		t.Fatalf("expected limit 10, got %d", v)
	}
}
