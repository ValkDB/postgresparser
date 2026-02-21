// helpers.go contains shared helper utilities used across the parser build steps.
package postgresparser

import (
	"strconv"
	"strings"

	"github.com/antlr4-go/antlr/v4"

	"github.com/valkdb/postgresparser/gen"
	"github.com/valkdb/postgresparser/internal/ident"
)

// columnRef is an internal representation of a column reference.
type columnRef struct {
	TableAlias string
	Name       string
	Expr       string
}

// --- Column Usage Analysis Helpers ---

// columnRefCollector walks the subtree and records every column reference encountered.
type columnRefCollector struct {
	*gen.BasePostgreSQLParserListener
	refs []*gen.ColumnrefContext
}

func (c *columnRefCollector) EnterColumnref(ctx *gen.ColumnrefContext) {
	c.refs = append(c.refs, ctx)
}

// windowClauseCollector walks the subtree and records window function OVER clauses.
type windowClauseCollector struct {
	*gen.BasePostgreSQLParserListener
	overClauses []*gen.Over_clauseContext
}

func (w *windowClauseCollector) EnterOver_clause(ctx *gen.Over_clauseContext) {
	w.overClauses = append(w.overClauses, ctx)
}

// findAndRecordUsage finds all column references within a given context and adds them to the ParsedQuery with the specified role.
// Note: For filter roles (WHERE/HAVING), prefer findAndRecordComparisons (helpers_comparison.go) which uses
// AST-based operator extraction. This function uses a string-heuristic fallback (extractOperatorFromExpression)
// that is adequate for non-filter roles (join, projection, group, order) but less accurate for filters.
func findAndRecordUsage(result *ParsedQuery, ctx antlr.RuleContext, role ColumnUsageType, tokens antlr.TokenStream) {
	if ctx == nil {
		return
	}
	collector := &columnRefCollector{BasePostgreSQLParserListener: &gen.BasePostgreSQLParserListener{}}
	tree, ok := ctx.(antlr.ParseTree)
	if !ok {
		return
	}
	antlr.ParseTreeWalkerDefault.Walk(collector, tree)

	// Extract operator once for the entire expression (not per column)
	var sharedOperator string
	if role == ColumnUsageTypeFilter || role == ColumnUsageTypeJoin {
		sharedOperator = extractOperatorFromExpression(ctx, tokens)
	}

	// Apply operator to only the first column reference to avoid duplicates
	operatorAssigned := false

	for _, colCtx := range collector.refs {
		ref := parseColRefFromContext(colCtx)
		usage := ColumnUsage{
			TableAlias: ref.TableAlias,
			Column:     ref.Name,
			Expression: colCtx.GetText(),
			UsageType:  role,
			Context:    ctx.GetText(), // Context is the text of the rule context that triggered the find
		}

		// Assign operator only to the first column to avoid duplicates
		if sharedOperator != "" && !operatorAssigned {
			usage.Operator = sharedOperator
			operatorAssigned = true
		}

		// Extract functions if any
		usage.Functions = extractFunctionsFromContext(ctx, colCtx, tokens)

		result.ColumnUsage = append(result.ColumnUsage, usage)
	}
}

// extractWindowFunctions finds all window functions and records their PARTITION BY and ORDER BY columns.
func extractWindowFunctions(result *ParsedQuery, ctx antlr.RuleContext, tokens antlr.TokenStream) {
	if ctx == nil {
		return
	}

	// Collect all OVER clauses
	collector := &windowClauseCollector{BasePostgreSQLParserListener: &gen.BasePostgreSQLParserListener{}}
	tree, ok := ctx.(antlr.ParseTree)
	if !ok {
		return
	}
	antlr.ParseTreeWalkerDefault.Walk(collector, tree)

	// Process each window function
	for _, overCtx := range collector.overClauses {
		if overCtx.Window_specification() != nil {
			windowSpec := overCtx.Window_specification()

			// Extract PARTITION BY columns
			if partitionClause := windowSpec.Partition_clause_(); partitionClause != nil {
				findAndRecordUsage(result, partitionClause, ColumnUsageTypeWindowPartition, tokens)
			}

			// Extract ORDER BY columns
			if sortClause := windowSpec.Sort_clause_(); sortClause != nil {
				findAndRecordUsage(result, sortClause, ColumnUsageTypeWindowOrder, tokens)
			}
		}
	}
}

// parseColRefFromContext extracts table alias and column name from a ColumnrefContext.
func parseColRefFromContext(ctx *gen.ColumnrefContext) columnRef {
	if ctx == nil {
		return columnRef{}
	}
	var parts []string
	if ctx.Colid() != nil {
		parts = append(parts, strings.TrimSpace(ctx.Colid().GetText()))
	}
	if ind := ctx.Indirection(); ind != nil {
		for _, el := range ind.AllIndirection_el() {
			if indCtx, ok := el.(*gen.Indirection_elContext); ok {
				if indCtx.Attr_name() != nil {
					parts = append(parts, strings.TrimSpace(indCtx.Attr_name().GetText()))
				} else if indCtx.STAR() != nil {
					parts = append(parts, "*")
				}
			}
		}
	}
	ref := columnRefFromParts(parts)
	if ref.Name == "" {
		ref.Name = strings.TrimSpace(ctx.GetText())
	}
	ref.Expr = ctx.GetText()
	return ref
}

func columnRefFromParts(parts []string) columnRef {
	if len(parts) == 0 {
		return columnRef{}
	}
	ref := columnRef{}
	switch len(parts) {
	case 1:
		ref.Name = ident.TrimQuotes(parts[0])
	default:
		ref.TableAlias = ident.TrimQuotes(parts[len(parts)-2])
		ref.Name = ident.TrimQuotes(parts[len(parts)-1])
	}
	return ref
}

func tableRefAliasOrName(tr TableRef) string {
	if tr.Alias != "" {
		return tr.Alias
	}
	if tr.Name != "" {
		return tr.Name
	}
	return strings.TrimSpace(tr.Raw)
}

// recordUsingJoinFromString parses a textual USING clause and records join usages for the two most recent base tables.
func recordUsingJoinFromString(result *ParsedQuery, clause string) {
	if result == nil {
		return
	}
	upper := strings.ToUpper(strings.TrimSpace(clause))
	if !strings.HasPrefix(upper, "USING") {
		return
	}
	start := strings.Index(clause, "(")
	end := strings.LastIndex(clause, ")")
	if start < 0 || end <= start {
		return
	}
	body := clause[start+1 : end]
	rawCols := strings.Split(body, ",")
	cols := make([]string, 0, len(rawCols))
	for _, c := range rawCols {
		col := ident.TrimQuotes(strings.TrimSpace(c))
		if col != "" {
			cols = append(cols, col)
		}
	}
	recordUsingJoinFromParts(result, cols, clause)
}

// recordUsingJoinFromParts associates the supplied column names with the left/right base tables of the current join context.
func recordUsingJoinFromParts(result *ParsedQuery, cols []string, context string) {
	if result == nil || len(cols) == 0 {
		return
	}
	var base []TableRef
	for i := len(result.Tables) - 1; i >= 0; i-- {
		if result.Tables[i].Type == TableTypeBase {
			base = append(base, result.Tables[i])
			if len(base) == 2 {
				break
			}
		}
	}
	if len(base) < 2 {
		return
	}
	left := base[1]
	right := base[0]
	leftAlias := ident.TrimQuotes(tableRefAliasOrName(left))
	rightAlias := ident.TrimQuotes(tableRefAliasOrName(right))
	for _, col := range cols {
		if col == "" {
			continue
		}
		result.ColumnUsage = append(result.ColumnUsage, ColumnUsage{
			TableAlias: leftAlias,
			Column:     col,
			Expression: col,
			UsageType:  ColumnUsageTypeJoin,
			Context:    context,
			Side:       "left",
		})
		result.ColumnUsage = append(result.ColumnUsage, ColumnUsage{
			TableAlias: rightAlias,
			Column:     col,
			Expression: col,
			UsageType:  ColumnUsageTypeJoin,
			Context:    context,
			Side:       "right",
		})
	}
}

// --- General Parser Helpers ---

// aliasFromAliasClause extracts the alias text from an alias_clause if present.
func aliasFromAliasClause(alias gen.IAlias_clauseContext, tokens antlr.TokenStream) string {
	if alias == nil {
		return ""
	}
	if alias.Colid() != nil {
		if prc, ok := alias.Colid().(antlr.ParserRuleContext); ok {
			return strings.TrimSpace(ctxText(tokens, prc))
		}
	}
	return ""
}

// aliasFromFuncAlias extracts the alias provided for set-returning functions.
func aliasFromFuncAlias(alias gen.IFunc_alias_clauseContext, tokens antlr.TokenStream) string {
	if alias == nil {
		return ""
	}
	if alias.Alias_clause() != nil {
		return aliasFromAliasClause(alias.Alias_clause(), tokens)
	}
	if alias.Colid() != nil {
		if prc, ok := alias.Colid().(antlr.ParserRuleContext); ok {
			return strings.TrimSpace(ctxText(tokens, prc))
		}
	}
	return ""
}

// ctxText returns the exact input substring covered by the supplied parse context.
func ctxText(tokens antlr.TokenStream, ctx antlr.RuleContext) string {
	if ctx == nil {
		return ""
	}
	ruleCtx, ok := ctx.(antlr.ParserRuleContext)
	if !ok {
		return ""
	}
	start := ruleCtx.GetStart()
	stop := ruleCtx.GetStop()
	if start == nil || stop == nil {
		return ""
	}
	interval := antlr.Interval{
		Start: start.GetTokenIndex(),
		Stop:  stop.GetTokenIndex(),
	}
	return tokens.GetTextFromInterval(interval)
}

// splitQualifiedName splits identifiers of the form schema.name into structured parts.
// It is quote-aware: dots inside double-quoted identifiers (e.g., "my.schema"."my.table")
// are not treated as separators.
func splitQualifiedName(name string) (string, string) {
	if name == "" {
		return "", ""
	}
	parts := splitQuotedDot(name)
	if len(parts) == 1 {
		return "", strings.TrimSpace(parts[0])
	}
	schema := strings.Join(parts[:len(parts)-1], ".")
	return strings.TrimSpace(schema), strings.TrimSpace(parts[len(parts)-1])
}

// splitQuotedDot splits a string on dots that are outside double-quoted regions.
func splitQuotedDot(s string) []string {
	var parts []string
	inQuote := false
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			inQuote = !inQuote
		case '.':
			if !inQuote {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// extractParameters tokenises the SQL to collect positional and anonymous parameters.
func extractParameters(sql string) []Parameter {
	input := antlr.NewInputStream(sql)
	lexer := gen.NewPostgreSQLLexer(input)
	var params []Parameter
	anonIndex := 1

	for {
		token := lexer.NextToken()
		if token == nil {
			break
		}
		if token.GetTokenType() == antlr.TokenEOF {
			break
		}
		if token.GetTokenType() != gen.PostgreSQLLexerPARAM {
			continue
		}
		text := token.GetText()
		p := Parameter{
			Raw: text,
		}
		if strings.HasPrefix(text, "$") {
			p.Marker = "$"
			if idx, err := strconv.Atoi(text[1:]); err == nil {
				p.Position = idx
			}
		} else {
			p.Marker = "?"
			p.Position = anonIndex
			anonIndex++
		}
		params = append(params, p)
	}
	return params
}

// extractOperatorFromExpression is a legacy string-heuristic fallback for operator detection.
// For filter/WHERE conditions, the AST-based findAndRecordComparisons (in helpers_comparison.go)
// is used instead, which walks the ANTLR parse tree for accurate operator extraction.
// This function is only used for non-filter roles (join, projection, group, order) via
// findAndRecordUsage, where operator detection is best-effort and less critical.
func extractOperatorFromExpression(exprCtx antlr.RuleContext, tokens antlr.TokenStream) string {
	// Get the text of the expression
	exprText := exprCtx.GetText()

	// Simple heuristic to extract operators from expressions
	// This looks for common SQL operators in the expression text
	operators := []string{"!=", "<>", "<=", ">=", "=", "<", ">", "LIKE", "ILIKE", "NOT IN", "IN", "IS NOT", "IS"}

	// Hoist case conversion above the loop so it is computed once.
	upperExpr := strings.ToUpper(exprText)

	for _, op := range operators {
		upperOp := strings.ToUpper(op)

		// Check if the operator exists in the expression
		if strings.Contains(upperExpr, upperOp) {
			// Special handling for word operators (LIKE, IN, IS) - ensure they're word boundaries
			if op == "LIKE" || op == "ILIKE" || op == "IN" || op == "NOT IN" || op == "IS" || op == "IS NOT" {
				// Check it's a word boundary (crude but effective)
				idx := strings.Index(upperExpr, upperOp)
				if idx > 0 {
					prevChar := upperExpr[idx-1]
					if prevChar != ' ' && prevChar != '(' && prevChar != ')' {
						continue // Not a word boundary
					}
				}
				if idx+len(upperOp) < len(upperExpr) {
					nextChar := upperExpr[idx+len(upperOp)]
					if nextChar != ' ' && nextChar != '(' && nextChar != ')' && nextChar != '\'' {
						continue // Not a word boundary
					}
				}
			}
			return strings.ToUpper(op)
		}
	}

	return ""
}

// functionCallCollector walks the subtree and records function calls.
type functionCallCollector struct {
	*gen.BasePostgreSQLParserListener
	functions []string
}

func (f *functionCallCollector) EnterFunc_application(ctx *gen.Func_applicationContext) {
	if ctx.Func_name() != nil {
		funcName := strings.ToUpper(ctx.Func_name().GetText())
		f.functions = append(f.functions, funcName)
	}
}

// extractFunctionsFromContext extracts function names that wrap the column reference.
// Note: Function attribution is heuristic (string containment based). A function is
// considered relevant if its name appears in the context text as "FUNC(" and the
// column text also appears in the same context. This may produce false positives for
// expressions where function and column text coincidentally co-occur.
func extractFunctionsFromContext(ctx antlr.RuleContext, colCtx antlr.RuleContext, tokens antlr.TokenStream) []string {
	// Walk the parse tree to find function calls
	collector := &functionCallCollector{BasePostgreSQLParserListener: &gen.BasePostgreSQLParserListener{}}
	tree, ok := ctx.(antlr.ParseTree)
	if !ok {
		return nil
	}
	antlr.ParseTreeWalkerDefault.Walk(collector, tree)

	// Hoist GetText() calls outside the loop to avoid redundant O(N) recomputation.
	ctxText := ctx.GetText()
	colText := colCtx.GetText()

	// Check if the column is inside any of these functions
	var relevantFunctions []string
	for _, funcName := range collector.functions {
		// Simple heuristic: if the function context contains the column, it's relevant
		if strings.Contains(ctxText, funcName+"(") &&
			strings.Contains(ctxText, colText) {
			relevantFunctions = append(relevantFunctions, funcName)
		}
	}

	return relevantFunctions
}
