package analysis

import (
	"sort"
	"strconv"
	"strings"

	"github.com/valkdb/postgresparser/internal/ident"
)

// BaseTables returns unique base relations referenced by the query.
// When a table has no explicit schema qualifier, BaseTables defaults the Schema
// field to "public". This is PostgreSQL-specific behavior reflecting the default
// search_path; callers targeting other databases should account for this.
func BaseTables(a *SQLAnalysis) []SQLTable {
	if a == nil {
		return nil
	}
	seen := map[string]struct{}{}
	var tables []SQLTable
	for _, tbl := range a.Tables {
		if tbl.Type != SQLTableTypeBase {
			continue
		}
		schema := ident.TrimQuotes(tbl.Schema)
		if schema == "" {
			schema = "public"
		}
		name := ident.TrimQuotes(tbl.Name)
		alias := ident.TrimQuotes(tbl.Alias)
		key := strings.ToLower(schema) + "." + strings.ToLower(name) + "|" + strings.ToLower(alias)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		tables = append(tables, SQLTable{Schema: schema, Name: name, Alias: alias, Type: tbl.Type})
	}
	return tables
}

// UsageByRoles filters ColumnUsage by the provided roles.
func UsageByRoles(a *SQLAnalysis, roles ...SQLUsageType) []SQLColumnUsage {
	if a == nil || len(a.ColumnUsage) == 0 {
		return nil
	}
	roleSet := map[SQLUsageType]struct{}{}
	for _, r := range roles {
		roleSet[r] = struct{}{}
	}
	var result []SQLColumnUsage
	for _, usage := range a.ColumnUsage {
		if len(roleSet) > 0 {
			if _, ok := roleSet[usage.UsageType]; !ok {
				continue
			}
		}
		result = append(result, usage)
	}
	return result
}

// ColumnsForTable returns distinct column names for the given table alias (or base table name),
// restricted to the provided roles. When alias is empty and only one base table exists,
// that table is assumed.
func ColumnsForTable(a *SQLAnalysis, alias string, roles ...SQLUsageType) []string {
	if a == nil {
		return nil
	}
	tables := BaseTables(a)
	single := len(tables) == 1
	normAlias := strings.ToLower(strings.TrimSpace(alias))
	if normAlias == "" && single {
		normAlias = strings.ToLower(tableAliasOrName(tables[0]))
	}
	seen := map[string]struct{}{}
	var ordered []string
	for _, usage := range UsageByRoles(a, roles...) {
		usageAlias := strings.ToLower(strings.TrimSpace(usage.TableAlias))
		if !matchesAlias(usageAlias, normAlias, single) {
			continue
		}
		name := strings.TrimSpace(usage.Column)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		ordered = append(ordered, name)
	}
	return ordered
}

// UsageStatsByTable returns counts of column usages per table alias for the given roles.
func UsageStatsByTable(a *SQLAnalysis, roles ...SQLUsageType) map[string]int {
	stats := map[string]int{}
	for _, usage := range UsageByRoles(a, roles...) {
		alias := strings.ToLower(strings.TrimSpace(usage.TableAlias))
		stats[alias]++
	}
	return stats
}

// LimitValue parses LIMIT into an integer (0 when ALL/unset).
func LimitValue(a *SQLAnalysis) int {
	if a == nil || a.Limit == nil {
		return 0
	}
	limit := strings.TrimSpace(a.Limit.Limit)
	limit = strings.TrimPrefix(strings.ToUpper(limit), "LIMIT ")
	limit = strings.TrimSpace(limit)
	if limit == "" || strings.EqualFold(limit, "ALL") {
		return 0
	}
	if v, err := strconv.Atoi(limit); err == nil {
		return v
	}
	return 0
}

// SortColumnUsage sorts usages by usage type then context for deterministic expectations.
func SortColumnUsage(usages []SQLColumnUsage) {
	sort.Slice(usages, func(i, j int) bool {
		if usages[i].UsageType != usages[j].UsageType {
			return usages[i].UsageType < usages[j].UsageType
		}
		if usages[i].TableAlias != usages[j].TableAlias {
			return usages[i].TableAlias < usages[j].TableAlias
		}
		if usages[i].Column != usages[j].Column {
			return usages[i].Column < usages[j].Column
		}
		return usages[i].Context < usages[j].Context
	})
}

func tableAliasOrName(t SQLTable) string {
	if t.Alias != "" {
		return t.Alias
	}
	return t.Name
}

// matchesAlias determines whether a column usage's table alias (candidate) matches
// the requested table alias (target) in the context of the query's table count.
//
// The matching rules are:
//   - If target is specified and candidate matches exactly, return true.
//   - If target is specified but candidate is empty and there is only one table,
//     return true (unqualified column in a single-table query is assumed to belong
//     to that table).
//   - If target is empty and there is only one table and candidate is also empty,
//     return true (both sides are unqualified in a single-table context).
//   - Otherwise return false.
func matchesAlias(candidate, target string, single bool) bool {
	if target != "" {
		if candidate == target {
			return true
		}
		return candidate == "" && single
	}
	return single && candidate == ""
}

// EntityNameFromTables creates an entity name string from base tables.
// Returns all real tables (not CTEs/subqueries) formatted as "schema.table".
// Multiple tables are comma-separated, e.g. "public.users,public.orders".
// Returns empty string if no base tables found.
func EntityNameFromTables(analysis *SQLAnalysis) string {
	if analysis == nil {
		return ""
	}

	tables := BaseTables(analysis)
	if len(tables) == 0 {
		return ""
	}

	// Build list of unique table names
	var tableNames []string
	seen := make(map[string]struct{}, len(tables))

	for _, table := range tables {
		schema := table.Schema

		// Create fully qualified table name
		fullName := schema + "." + table.Name

		// Skip duplicates
		if _, exists := seen[fullName]; exists {
			continue
		}
		seen[fullName] = struct{}{}
		tableNames = append(tableNames, fullName)
	}

	// Sort for consistent output
	sort.Strings(tableNames)

	// Join all table names
	return strings.Join(tableNames, ",")
}
