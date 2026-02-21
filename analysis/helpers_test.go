package analysis

import (
	"strings"
	"testing"

	"github.com/valkdb/postgresparser"
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

// TestBaseTables_TrimQuotes verifies BaseTables trims double quotes in external DTO table metadata.
func TestBaseTables_TrimQuotes(t *testing.T) {
	analysis := &SQLAnalysis{
		Tables: []SQLTable{
			{
				Schema: `"Public"`,
				Name:   `"Users"`,
				Alias:  `"U"`,
				Type:   SQLTableTypeBase,
			},
			{
				Name: `"cte_data"`,
				Type: SQLTableTypeCTE,
			},
		},
	}

	base := BaseTables(analysis)
	if len(base) != 1 {
		t.Fatalf("expected 1 base table, got %d (%+v)", len(base), base)
	}

	if strings.Contains(base[0].Schema, `"`) || strings.Contains(base[0].Name, `"`) || strings.Contains(base[0].Alias, `"`) {
		t.Fatalf("expected quotes to be trimmed from base table metadata, got %+v", base[0])
	}
	if base[0].Schema != "Public" || base[0].Name != "Users" || base[0].Alias != "U" {
		t.Fatalf("unexpected base table metadata: %+v", base[0])
	}
}

// TestConvertTables_ExternalDTO verifies table DTO conversion remains stable.
func TestConvertTables_ExternalDTO(t *testing.T) {
	input := []postgresparser.TableRef{
		{
			Schema: `"Public"`,
			Name:   `"Users"`,
			Alias:  `"U"`,
			Type:   postgresparser.TableTypeBase,
			Raw:    `"Public"."Users" AS "U"`,
		},
	}

	out := convertTables(input)
	if len(out) != 1 {
		t.Fatalf("expected 1 converted table, got %d", len(out))
	}

	if out[0].Schema != input[0].Schema ||
		out[0].Name != input[0].Name ||
		out[0].Alias != input[0].Alias ||
		out[0].Raw != input[0].Raw {
		t.Fatalf("conversion should preserve table DTO fields, got %+v from %+v", out[0], input[0])
	}
	if out[0].Type != SQLTableType(input[0].Type) {
		t.Fatalf("expected type %q, got %q", input[0].Type, out[0].Type)
	}
}
