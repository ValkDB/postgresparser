// Package analysis provides query analysis for the PostgreSQL parser.
// This file converts the parser IR (ParsedQuery) into a standalone SQLAnalysis DTO.
package analysis

import (
	"fmt"
	"strings"

	"github.com/valkdb/postgresparser"
)

// AnalyzeSQL parses and analyzes only the first SQL statement in the input.
// Use AnalyzeSQLAll for full multi-statement analysis, or AnalyzeSQLStrict to
// enforce exactly one statement.
func AnalyzeSQL(sql string) (*SQLAnalysis, error) {
	return AnalyzeSQLWithOptions(sql, postgresparser.ParseOptions{})
}

// AnalyzeSQLWithOptions parses and analyzes only the first SQL statement while
// enabling optional metadata extraction flags.
func AnalyzeSQLWithOptions(sql string, opts postgresparser.ParseOptions) (*SQLAnalysis, error) {
	pq, err := postgresparser.ParseSQLWithOptions(sql, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}
	return convertParsedQuery(pq), nil
}

// AnalyzeSQLAll parses all SQL statements and returns a batch analysis result.
func AnalyzeSQLAll(sql string) (*SQLAnalysisBatchResult, error) {
	return AnalyzeSQLAllWithOptions(sql, postgresparser.ParseOptions{})
}

// AnalyzeSQLAllWithOptions parses all SQL statements and enables optional
// metadata extraction flags.
func AnalyzeSQLAllWithOptions(sql string, opts postgresparser.ParseOptions) (*SQLAnalysisBatchResult, error) {
	batch, err := postgresparser.ParseSQLAllWithOptions(sql, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}
	return convertBatchResult(batch), nil
}

// AnalyzeSQLStrict parses SQL only when it contains exactly one statement.
func AnalyzeSQLStrict(sql string) (*SQLAnalysis, error) {
	return AnalyzeSQLStrictWithOptions(sql, postgresparser.ParseOptions{})
}

// AnalyzeSQLStrictWithOptions parses SQL only when it contains exactly one
// statement and enables optional metadata extraction flags.
func AnalyzeSQLStrictWithOptions(sql string, opts postgresparser.ParseOptions) (*SQLAnalysis, error) {
	pq, err := postgresparser.ParseSQLStrictWithOptions(sql, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}
	return convertParsedQuery(pq), nil
}

// convertBatchResult maps parser ParseBatchResult into analysis batch DTO.
func convertBatchResult(batch *postgresparser.ParseBatchResult) *SQLAnalysisBatchResult {
	if batch == nil {
		return nil
	}
	res := &SQLAnalysisBatchResult{
		Statements:  convertStatementResults(batch.Statements),
		HasFailures: batch.HasFailures,
	}
	return res
}

// convertStatementResults maps parser statement results into analysis statement
// results.
func convertStatementResults(statements []postgresparser.StatementParseResult) []SQLStatementAnalysisResult {
	if len(statements) == 0 {
		return nil
	}
	out := make([]SQLStatementAnalysisResult, 0, len(statements))
	for _, stmt := range statements {
		var query *SQLAnalysis
		if stmt.Query != nil {
			query = convertParsedQuery(stmt.Query)
		}
		out = append(out, SQLStatementAnalysisResult{
			Index:    stmt.Index,
			RawSQL:   stmt.RawSQL,
			Query:    query,
			Warnings: convertWarnings(stmt.Warnings),
		})
	}
	return out
}

// convertWarnings maps parser parse warnings into analysis parse warnings.
func convertWarnings(warnings []postgresparser.ParseWarning) []SQLParseWarning {
	if len(warnings) == 0 {
		return nil
	}
	out := make([]SQLParseWarning, 0, len(warnings))
	for _, w := range warnings {
		out = append(out, SQLParseWarning{
			Code:    SQLParseWarningCode(w.Code),
			Message: w.Message,
		})
	}
	return out
}

// convertParsedQuery maps a parser ParsedQuery into the public SQLAnalysis DTO.
func convertParsedQuery(pq *postgresparser.ParsedQuery) *SQLAnalysis {
	if pq == nil {
		return nil
	}
	res := &SQLAnalysis{
		RawSQL:        pq.RawSQL,
		Command:       SQLCommand(pq.Command),
		Where:         append([]string(nil), pq.Where...),
		Having:        append([]string(nil), pq.Having...),
		GroupBy:       append([]string(nil), pq.GroupBy...),
		JoinClauses:   append([]string(nil), pq.JoinConditions...),
		Parameters:    convertParameters(pq.Parameters),
		InsertColumns: append([]string(nil), pq.InsertColumns...),
		SetClauses:    append([]string(nil), pq.SetClauses...),
	}

	res.Tables = convertTables(pq.Tables)
	res.Columns = convertColumns(pq.Columns)
	res.SetOperations = convertSetOperations(pq.SetOperations)
	res.Subqueries = convertSubqueries(pq.Subqueries)
	res.CTEs = convertCTEs(pq.CTEs)
	res.OrderBy = convertOrderBy(pq.OrderBy)
	res.Limit = convertLimit(pq.Limit)
	res.Upsert = convertUpsert(pq.Upsert)
	res.Returning = normalizeReturning(pq.Returning)
	res.Merge = convertMerge(pq.Merge)
	res.DDLActions = convertDDLActions(pq.DDLActions)
	res.ColumnUsage = convertColumnUsage(pq.ColumnUsage)
	res.Correlations = convertCorrelations(pq.Correlations)
	if pq.DerivedColumns != nil {
		res.DerivedColumns = make(map[string]string, len(pq.DerivedColumns))
		for k, v := range pq.DerivedColumns {
			res.DerivedColumns[k] = v
		}
	}

	return res
}

// convertColumnUsage maps parser column-usage entries into analysis column usage entries.
func convertColumnUsage(usage []postgresparser.ColumnUsage) []SQLColumnUsage {
	if len(usage) == 0 {
		return nil
	}
	out := make([]SQLColumnUsage, 0, len(usage))
	for _, u := range usage {
		out = append(out, SQLColumnUsage{
			TableAlias: u.TableAlias,
			Column:     u.Column,
			Expression: u.Expression,
			UsageType:  SQLUsageType(u.UsageType),
			Context:    u.Context,
			Operator:   u.Operator,
			Side:       u.Side,
			Functions:  append([]string(nil), u.Functions...),
		})
	}
	return out
}

// convertTables maps parser table references into analysis table references.
func convertTables(tbls []postgresparser.TableRef) []SQLTable {
	if len(tbls) == 0 {
		return nil
	}
	out := make([]SQLTable, 0, len(tbls))
	for _, t := range tbls {
		out = append(out, SQLTable{
			Schema: t.Schema,
			Name:   t.Name,
			Alias:  t.Alias,
			Type:   SQLTableType(t.Type),
			Raw:    t.Raw,
		})
	}
	return out
}

// convertColumns maps parser SELECT columns into analysis SELECT columns.
func convertColumns(cols []postgresparser.SelectColumn) []SQLColumn {
	if len(cols) == 0 {
		return nil
	}
	out := make([]SQLColumn, 0, len(cols))
	for _, c := range cols {
		out = append(out, SQLColumn{
			Expression: c.Expression,
			Alias:      c.Alias,
		})
	}
	return out
}

// convertSetOperations maps parser set-operation metadata into analysis set-operation metadata.
func convertSetOperations(sets []postgresparser.SetOperation) []SQLSetOperation {
	if len(sets) == 0 {
		return nil
	}
	out := make([]SQLSetOperation, 0, len(sets))
	for _, s := range sets {
		out = append(out, SQLSetOperation{
			Type:    SQLSetOperationType(s.Type),
			Query:   s.Query,
			Columns: append([]string(nil), s.Columns...),
			Tables:  convertTables(s.Tables),
		})
	}
	return out
}

// convertSubqueries maps parser subquery references into analysis subquery references.
func convertSubqueries(subs []postgresparser.SubqueryRef) []SQLSubquery {
	if len(subs) == 0 {
		return nil
	}
	out := make([]SQLSubquery, 0, len(subs))
	for _, s := range subs {
		out = append(out, SQLSubquery{
			Alias:    s.Alias,
			Analysis: convertParsedQuery(s.Query),
		})
	}
	return out
}

// convertCTEs maps parser CTE metadata into analysis CTE metadata.
func convertCTEs(ctes []postgresparser.CTE) []SQLCTE {
	if len(ctes) == 0 {
		return nil
	}
	out := make([]SQLCTE, 0, len(ctes))
	for _, cte := range ctes {
		out = append(out, SQLCTE{
			Name:         cte.Name,
			Query:        cte.Query,
			Materialized: cte.Materialized,
		})
	}
	return out
}

// convertOrderBy maps parser ORDER BY expressions into analysis ORDER BY expressions.
func convertOrderBy(items []postgresparser.OrderExpression) []SQLOrderExpression {
	if len(items) == 0 {
		return nil
	}
	out := make([]SQLOrderExpression, 0, len(items))
	for _, item := range items {
		out = append(out, SQLOrderExpression{
			Expression: item.Expression,
			Direction:  item.Direction,
			Nulls:      item.Nulls,
		})
	}
	return out
}

// convertLimit maps a parser LIMIT clause into an analysis LIMIT clause.
func convertLimit(limit *postgresparser.LimitClause) *SQLLimit {
	if limit == nil {
		return nil
	}
	return &SQLLimit{
		Limit:    limit.Limit,
		Offset:   limit.Offset,
		IsNested: limit.IsNested,
	}
}

// convertParameters maps parser parameter placeholders into analysis parameters.
func convertParameters(params []postgresparser.Parameter) []SQLParameter {
	if len(params) == 0 {
		return nil
	}
	out := make([]SQLParameter, 0, len(params))
	for _, p := range params {
		out = append(out, SQLParameter{
			Raw:      p.Raw,
			Marker:   p.Marker,
			Position: p.Position,
		})
	}
	return out
}

// convertCorrelations maps parser join-correlation metadata into analysis correlations.
func convertCorrelations(corrs []postgresparser.JoinCorrelation) []SQLJoinCorrelation {
	if len(corrs) == 0 {
		return nil
	}
	out := make([]SQLJoinCorrelation, 0, len(corrs))
	for _, c := range corrs {
		out = append(out, SQLJoinCorrelation{
			OuterAlias: c.OuterAlias,
			InnerAlias: c.InnerAlias,
			Expression: c.Expression,
			Type:       c.Type,
		})
	}
	return out
}

// convertUpsert maps parser UPSERT metadata into analysis UPSERT metadata.
func convertUpsert(upsert *postgresparser.UpsertClause) *SQLUpsert {
	if upsert == nil {
		return nil
	}
	return &SQLUpsert{
		TargetColumns: append([]string(nil), upsert.TargetColumns...),
		TargetWhere:   upsert.TargetWhere,
		Constraint:    upsert.Constraint,
		Action:        upsert.Action,
		SetClauses:    append([]string(nil), upsert.SetClauses...),
		ActionWhere:   upsert.ActionWhere,
	}
}

// convertMerge maps parser MERGE metadata into analysis MERGE metadata.
func convertMerge(merge *postgresparser.MergeClause) *SQLMerge {
	if merge == nil {
		return nil
	}
	out := &SQLMerge{
		Target:    convertMergeTable(merge.Target),
		Source:    SQLMergeSource{},
		Condition: merge.Condition,
		Actions:   convertMergeActions(merge.Actions),
	}
	if merge.Source.Table.Schema != "" || merge.Source.Table.Name != "" {
		table := convertMergeTable(merge.Source.Table)
		out.Source.Table = &table
	}
	if merge.Source.Subquery != nil {
		out.Source.Subquery = &SQLSubquery{
			Alias:    merge.Source.Subquery.Alias,
			Analysis: convertParsedQuery(merge.Source.Subquery.Query),
		}
	}
	return out
}

// normalizeReturning strips optional RETURNING prefixes and flattens comma-delimited items.
func normalizeReturning(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	var out []string
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		upper := strings.ToUpper(trimmed)
		if strings.HasPrefix(upper, "RETURNING ") {
			trimmed = strings.TrimSpace(trimmed[len("RETURNING "):])
		}
		parts := splitCommasRespectingParens(trimmed)
		for _, part := range parts {
			cleaned := strings.TrimSpace(part)
			if cleaned != "" {
				out = append(out, cleaned)
			}
		}
	}
	return out
}

// splitCommasRespectingParens splits a string on commas that are not inside
// parentheses or single-quoted string literals. This prevents incorrect splitting
// of function arguments like "func(a, b)" into ["func(a", "b)"] and also handles
// string literals like "concat('a,b', name)" correctly.
func splitCommasRespectingParens(s string) []string {
	var parts []string
	depth := 0
	inQuote := false
	start := 0
	for i, ch := range s {
		switch ch {
		case '\'':
			inQuote = !inQuote
		case '(':
			if !inQuote {
				depth++
			}
		case ')':
			if !inQuote && depth > 0 {
				depth--
			}
		case ',':
			if !inQuote && depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// convertMergeTable maps a parser table reference into an analysis table reference for MERGE.
func convertMergeTable(tbl postgresparser.TableRef) SQLTable {
	return SQLTable{
		Schema: tbl.Schema,
		Name:   tbl.Name,
		Alias:  tbl.Alias,
		Type:   SQLTableType(tbl.Type),
		Raw:    tbl.Raw,
	}
}

// convertDDLActions maps parser DDL actions into analysis DDL actions.
func convertDDLActions(actions []postgresparser.DDLAction) []SQLDDLAction {
	if len(actions) == 0 {
		return nil
	}
	out := make([]SQLDDLAction, 0, len(actions))
	for _, a := range actions {
		out = append(out, SQLDDLAction{
			Type:          string(a.Type),
			ObjectName:    a.ObjectName,
			ObjectType:    a.ObjectType,
			Schema:        a.Schema,
			Columns:       append([]string(nil), a.Columns...),
			ColumnDetails: convertDDLColumns(a.ColumnDetails),
			Flags:         append([]string(nil), a.Flags...),
			IndexType:     a.IndexType,
			Target:        a.Target,
			Comment:       a.Comment,
		})
	}
	return out
}

// convertDDLColumns maps parser CREATE TABLE column metadata into analysis DTOs.
func convertDDLColumns(cols []postgresparser.DDLColumn) []SQLDDLColumn {
	if len(cols) == 0 {
		return nil
	}
	out := make([]SQLDDLColumn, 0, len(cols))
	for _, c := range cols {
		out = append(out, SQLDDLColumn{
			Name:     c.Name,
			Type:     c.Type,
			Nullable: c.Nullable,
			Default:  c.Default,
			Comment:  append([]string(nil), c.Comment...),
		})
	}
	return out
}

// convertMergeActions maps parser MERGE actions into analysis MERGE actions.
func convertMergeActions(actions []postgresparser.MergeAction) []SQLMergeAction {
	if len(actions) == 0 {
		return nil
	}
	out := make([]SQLMergeAction, 0, len(actions))
	for _, act := range actions {
		out = append(out, SQLMergeAction{
			Type:          act.Type,
			Condition:     act.Condition,
			SetClauses:    append([]string(nil), act.SetClauses...),
			InsertColumns: append([]string(nil), act.InsertColumns...),
			InsertValues:  act.InsertValues,
		})
	}
	return out
}
