// select.go contains logic for populating SELECT-specific metadata in the IR.
package postgresparser

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/antlr4-go/antlr/v4"

	"github.com/valkdb/postgresparser/gen"
)

// populateSelect builds SELECT metadata from the given ANTLR context.
func populateSelect(result *ParsedQuery, selectCtx gen.ISelectstmtContext, tokens antlr.TokenStream) error {
	withClause, simple, selectNoParens, err := resolveSelect(selectCtx)
	if err != nil {
		return err
	}
	return populateSelectFromResolved(result, withClause, simple, selectNoParens, tokens)
}

// populateSelectFromResolved fills the ParsedQuery using pre-resolved SELECT components.
func populateSelectFromResolved(result *ParsedQuery, withClause gen.IWith_clauseContext, simple gen.ISimple_select_pramaryContext,
	selectNoParens gen.ISelect_no_parensContext, tokens antlr.TokenStream) error {
	return populateSelectFromResolvedNested(result, withClause, simple, selectNoParens, tokens, false)
}

// populateSelectFromResolvedNested fills the ParsedQuery with nesting awareness.
func populateSelectFromResolvedNested(result *ParsedQuery, withClause gen.IWith_clauseContext, simple gen.ISimple_select_pramaryContext,
	selectNoParens gen.ISelect_no_parensContext, tokens antlr.TokenStream, isNested bool) error {
	if result == nil {
		return fmt.Errorf("select result container: %w", ErrNilContext)
	}

	cteNames := map[string]struct{}{}
	if withClause != nil {
		ctes, cteTables := extractCTEs(withClause, tokens)
		if len(ctes) > 0 {
			result.CTEs = append(result.CTEs, ctes...)
		}
		// Add tables found within CTEs to the result
		if len(cteTables) > 0 {
			result.Tables = append(result.Tables, cteTables...)
		}
	}
	for _, cte := range result.CTEs {
		if cte.Name != "" {
			cteNames[strings.ToLower(cte.Name)] = struct{}{}
		}
	}

	if simple == nil {
		return fmt.Errorf("unsupported SELECT form")
	}

	extractProjection(result, simple, tokens)
	extractFromClause(result, simple.From_clause(), tokens, cteNames)
	extractWhereClause(result, simple.Where_clause(), tokens)
	extractHavingClause(result, simple.Having_clause(), tokens)
	extractGroupClause(result, simple.Group_clause(), tokens)
	extractOrderClause(result, selectNoParens.Sort_clause_(), tokens)
	extractLimitClause(result, selectNoParens, tokens, isNested) // Use the isNested parameter

	setOps, leadingTables, opSubqueries := extractSetOperationsWithResult(selectNoParens, tokens, cteNames, result)
	if len(setOps) > 0 {
		result.SetOperations = append(result.SetOperations, setOps...)
	}
	appendSetOpTables(result, setOps, leadingTables)
	if len(opSubqueries) > 0 {
		result.Subqueries = append(result.Subqueries, opSubqueries...)
	}

	return nil
}

// resolveSelect unwraps nested structures to expose the primary SELECT clauses.
func resolveSelect(selectCtx gen.ISelectstmtContext) (gen.IWith_clauseContext, gen.ISimple_select_pramaryContext, gen.ISelect_no_parensContext, error) {
	if selectCtx == nil {
		return nil, nil, nil, fmt.Errorf("select statement: %w", ErrNilContext)
	}

	if snp := selectCtx.Select_no_parens(); snp != nil {
		selectClause := snp.Select_clause()
		if selectClause == nil {
			return snp.With_clause(), nil, snp, fmt.Errorf("missing select clause")
		}
		simpleIntersect := selectClause.Simple_select_intersect(0)
		if simpleIntersect == nil {
			return snp.With_clause(), nil, snp, fmt.Errorf("missing simple select")
		}
		simple := simpleIntersect.Simple_select_pramary(0)
		if simple == nil {
			return snp.With_clause(), nil, snp, fmt.Errorf("missing simple select primary")
		}
		return snp.With_clause(), simple, snp, nil
	}

	if swp := selectCtx.Select_with_parens(); swp != nil {
		return resolveSelectFromParens(swp)
	}

	return nil, nil, nil, fmt.Errorf("unable to resolve select statement")
}

// resolveSelectFromParens collapses parenthesised selects until a base form is reached.
func resolveSelectFromParens(swp gen.ISelect_with_parensContext) (gen.IWith_clauseContext, gen.ISimple_select_pramaryContext, gen.ISelect_no_parensContext, error) {
	current := swp
	for current != nil {
		if inner := current.Select_no_parens(); inner != nil {
			selectClause := inner.Select_clause()
			if selectClause == nil {
				return nil, nil, nil, fmt.Errorf("missing select clause")
			}
			simpleIntersect := selectClause.Simple_select_intersect(0)
			if simpleIntersect == nil {
				return nil, nil, nil, fmt.Errorf("missing simple select")
			}
			simple := simpleIntersect.Simple_select_pramary(0)
			if simple == nil {
				return nil, nil, nil, fmt.Errorf("missing simple select primary")
			}
			return nil, simple, inner, nil
		}
		current = current.Select_with_parens()
	}
	return nil, nil, nil, fmt.Errorf("unable to unwrap parenthesized select")
}

// extractCTEs captures metadata for each common table expression defined in WITH.
// It also builds nested IR for the CTE body and surfaces tables referenced within it.
func extractCTEs(withCtx gen.IWith_clauseContext, tokens antlr.TokenStream) ([]CTE, []TableRef) {
	if withCtx == nil {
		return nil, nil
	}
	listCtx := withCtx.Cte_list()
	if listCtx == nil {
		return nil, nil
	}
	commonExprs := listCtx.AllCommon_table_expr()
	ctes := make([]CTE, 0, len(commonExprs))
	var allTables []TableRef

	for _, cteCtx := range commonExprs {
		if cteCtx == nil {
			continue
		}
		name := ""
		if cteCtx.Name() != nil {
			if prc, ok := cteCtx.Name().(antlr.ParserRuleContext); ok {
				name = strings.TrimSpace(ctxText(tokens, prc))
			}
		}
		materialized := ""
		if cteCtx.Materialized_() != nil {
			if prc, ok := cteCtx.Materialized_().(antlr.ParserRuleContext); ok {
				materialized = strings.TrimSpace(ctxText(tokens, prc))
			}
		}
		query := ""
		var parsedQuery *ParsedQuery
		if cteCtx.Preparablestmt() != nil {
			if prc, ok := cteCtx.Preparablestmt().(antlr.ParserRuleContext); ok {
				query = strings.TrimSpace(ctxText(tokens, prc))
			}

			if stmtCtx := cteCtx.Preparablestmt(); stmtCtx != nil {
				parsed, err := parsePreparableStmtToIR(stmtCtx, tokens, query, ParseOptions{})
				if err == nil {
					parsedQuery = parsed
					if tables := extractTablesFromParsedQuery(parsedQuery); len(tables) > 0 {
						allTables = append(allTables, tables...)
					}
				} else if tables := extractTablesFromPreparableStmt(stmtCtx, tokens); len(tables) > 0 {
					allTables = append(allTables, tables...)
				}
			}
		}
		if name == "" {
			name = fmt.Sprintf("cte_%d", len(ctes)+1)
		}
		ctes = append(ctes, CTE{
			Name:         name,
			Query:        query,
			ParsedQuery:  parsedQuery,
			Materialized: materialized,
		})
	}

	return ctes, allTables
}

// extractTablesFromPreparableStmt extracts table references from a preparable statement (used in CTEs)
func extractTablesFromPreparableStmt(stmt gen.IPreparablestmtContext, tokens antlr.TokenStream) []TableRef {
	if stmt == nil {
		return nil
	}

	rawSQL := ""
	if prc, ok := stmt.(antlr.ParserRuleContext); ok {
		rawSQL = strings.TrimSpace(ctxText(tokens, prc))
	}

	parsed, err := parsePreparableStmtToIR(stmt, tokens, rawSQL, ParseOptions{})
	if err != nil {
		return nil
	}
	return extractTablesFromParsedQuery(parsed)
}

func extractTablesFromParsedQuery(parsed *ParsedQuery) []TableRef {
	if parsed == nil {
		return nil
	}

	tables := append([]TableRef(nil), parsed.Tables...)
	for _, subq := range parsed.Subqueries {
		if subq.Query != nil {
			tables = append(tables, extractTablesFromParsedQuery(subq.Query)...)
		}
	}
	return tables
}

// extractProjection records projection expressions and aliases for the SELECT list.
func extractProjection(result *ParsedQuery, simple gen.ISimple_select_pramaryContext, tokens antlr.TokenStream) {
	if simple == nil {
		return
	}
	targetList := simple.Target_list()
	if targetList == nil && simple.Target_list_() != nil {
		targetList = simple.Target_list_().Target_list()
	}
	if targetList == nil && simple.Select_with_parens() != nil {
		_, nestedSimple, _, err := resolveSelectFromParens(simple.Select_with_parens())
		if err == nil {
			extractProjection(result, nestedSimple, tokens)
		}
		return
	}
	if targetList == nil {
		return
	}

	for _, item := range targetList.AllTarget_el() {
		switch col := item.(type) {
		case *gen.Target_labelContext:
			expr := ""
			if col.A_expr() != nil {
				if prc, ok := col.A_expr().(antlr.ParserRuleContext); ok {
					expr = strings.TrimSpace(ctxText(tokens, prc))
				}
				findAndRecordUsage(result, col.A_expr(), ColumnUsageTypeProjection, tokens)
				extractExpressionSubqueries(result, col.A_expr(), tokens)
			}
			alias := ""
			switch {
			case col.ColLabel() != nil:
				if prc, ok := col.ColLabel().(antlr.ParserRuleContext); ok {
					alias = strings.TrimSpace(ctxText(tokens, prc))
				}
			case col.BareColLabel() != nil:
				if prc, ok := col.BareColLabel().(antlr.ParserRuleContext); ok {
					alias = strings.TrimSpace(ctxText(tokens, prc))
				}
			}
			result.Columns = append(result.Columns, SelectColumn{
				Expression: expr,
				Alias:      alias,
			})
			// Track derived columns (alias -> expression mapping)
			if alias != "" && expr != "" && alias != expr {
				result.DerivedColumns[alias] = expr
			}
		case *gen.Target_starContext:
			result.Columns = append(result.Columns, SelectColumn{
				Expression: strings.TrimSpace(ctxText(tokens, col)),
			})
		default:
			if prc, ok := col.(antlr.ParserRuleContext); ok {
				result.Columns = append(result.Columns, SelectColumn{
					Expression: strings.TrimSpace(ctxText(tokens, prc)),
				})
			}
		}
	}

	// Extract window functions from the projection
	extractWindowFunctions(result, targetList, tokens)
}

// extractFromClause walks a FROM clause to collect table references.
func extractFromClause(result *ParsedQuery, fromCtx gen.IFrom_clauseContext, tokens antlr.TokenStream, cteNames map[string]struct{}) {
	if fromCtx == nil {
		return
	}
	extractFromList(result, fromCtx.From_list(), tokens, cteNames)
}

// extractFromList iterates a from_list to accumulate table_ref nodes.
func extractFromList(result *ParsedQuery, listCtx gen.IFrom_listContext, tokens antlr.TokenStream, cteNames map[string]struct{}) {
	if listCtx == nil {
		return
	}
	for _, tbl := range listCtx.AllTable_ref() {
		collectTableRefs(result, tbl, tokens, cteNames)
	}
}

// collectTableRefs registers table, function, or subquery references within a join tree.
func collectTableRefs(result *ParsedQuery, ref gen.ITable_refContext, tokens antlr.TokenStream, cteNames map[string]struct{}) {
	if ref == nil {
		return
	}

	if rel := ref.Relation_expr(); rel != nil {
		name := ""
		if rel.Qualified_name() != nil {
			if prc, ok := rel.Qualified_name().(antlr.ParserRuleContext); ok {
				name = strings.TrimSpace(ctxText(tokens, prc))
			}
		}
		schema, relation := splitQualifiedName(name)
		alias := aliasFromAliasClause(ref.Alias_clause(), tokens)
		tableType := TableTypeBase
		if _, ok := cteNames[strings.ToLower(relation)]; ok {
			tableType = TableTypeCTE
		}

		rawText := ""
		if prc, ok := rel.(antlr.ParserRuleContext); ok {
			rawText = strings.TrimSpace(ctxText(tokens, prc))
		}
		result.Tables = append(result.Tables, TableRef{
			Schema: schema,
			Name:   relation,
			Alias:  alias,
			Type:   tableType,
			Raw:    rawText,
		})
	} else if fn := ref.Func_table(); fn != nil {
		tableName := ""
		if prc, ok := fn.(antlr.ParserRuleContext); ok {
			tableName = strings.TrimSpace(ctxText(tokens, prc))
		}
		alias := aliasFromFuncAlias(ref.Func_alias_clause(), tokens)
		result.Tables = append(result.Tables, TableRef{
			Name:  tableName,
			Alias: alias,
			Type:  TableTypeFunction,
			Raw:   tableName,
		})
		// Check for LATERAL correlation
		if prc, ok := ref.(antlr.ParserRuleContext); ok {
			if strings.Contains(strings.ToUpper(ctxText(tokens, prc)), "LATERAL") {
				detectLateralCorrelation(result, fn, tokens)
			}
		}
	} else if sub := ref.Select_with_parens(); sub != nil {
		alias := aliasFromAliasClause(ref.Alias_clause(), tokens)
		raw := ""
		if prc, ok := sub.(antlr.ParserRuleContext); ok {
			raw = strings.TrimSpace(ctxText(tokens, prc))
		}
		result.Tables = append(result.Tables, TableRef{
			Name:  alias,
			Alias: alias,
			Type:  TableTypeSubquery,
			Raw:   raw,
		})
		// Build nested subquery analysis without flattening inner column usage
		// into the parent query scope.
		if subRef, err := buildSubqueryRef(alias, sub, tokens); err == nil && subRef != nil {
			result.Subqueries = append(result.Subqueries, *subRef)
			appendSetOpTables(result, nil, subRef.Query.Tables)
		}
	}

	metas := resolveJoinMeta(ref, tokens)
	nestedRefs := ref.AllTable_ref()
	for i, nested := range nestedRefs {
		before := len(result.Tables)
		collectTableRefs(result, nested, tokens, cteNames)
		if i < len(metas) && before < len(result.Tables) {
			result.Tables[before].JoinType = metas[i].joinType
			result.Tables[before].JoinCondition = metas[i].joinCondition
		}
	}

	for _, join := range ref.AllJoin_qual() {
		joinCtx, ok := join.(antlr.ParserRuleContext)
		if !ok {
			continue
		}
		clauseText := strings.TrimSpace(ctxText(tokens, joinCtx))
		if clauseText != "" {
			result.JoinConditions = append(result.JoinConditions, clauseText)
		}
		if join.USING() != nil {
			recordUsingJoinFromString(result, clauseText)
		} else {
			findAndRecordUsage(result, joinCtx, ColumnUsageTypeJoin, tokens)
		}
	}
}

// joinMeta holds the resolved join type and condition for a nested table_ref.
type joinMeta struct {
	joinType      string
	joinCondition string
}

// resolveJoinMeta walks the children of a table_ref node in parse order to
// determine the join type and condition for each nested table_ref.
// The returned slice is parallel to ref.AllTable_ref().
func resolveJoinMeta(ref gen.ITable_refContext, tokens antlr.TokenStream) []joinMeta {
	prc, ok := ref.(antlr.ParserRuleContext)
	if !ok {
		return nil
	}

	hasOpenParen := ref.OPEN_PAREN() != nil
	seenFirstTableRef := false
	pendingType := ""
	var metas []joinMeta

	for _, child := range prc.GetChildren() {
		switch c := child.(type) {
		case antlr.TerminalNode:
			switch c.GetSymbol().GetTokenType() {
			case gen.PostgreSQLParserCROSS:
				pendingType = "CROSS"
			case gen.PostgreSQLParserNATURAL:
				pendingType = "NATURAL"
			}
		case *gen.Join_typeContext:
			jt := joinTypeFromCtx(c)
			if strings.HasPrefix(pendingType, "NATURAL") {
				pendingType = "NATURAL " + jt
			} else {
				pendingType = jt
			}
		case *gen.Table_refContext:
			if hasOpenParen && !seenFirstTableRef {
				// Base table inside parentheses — no join type.
				metas = append(metas, joinMeta{})
				seenFirstTableRef = true
			} else {
				if pendingType == "" {
					pendingType = "JOIN"
				}
				metas = append(metas, joinMeta{joinType: pendingType})
			}
			pendingType = ""
		case *gen.Join_qualContext:
			if len(metas) > 0 {
				metas[len(metas)-1].joinCondition = strings.TrimSpace(ctxText(tokens, c))
			}
		}
	}

	return metas
}

// joinTypeFromCtx extracts a normalised join type string from a join_type rule context.
func joinTypeFromCtx(ctx gen.IJoin_typeContext) string {
	if ctx.FULL() != nil {
		return "FULL"
	}
	if ctx.LEFT() != nil {
		return "LEFT"
	}
	if ctx.RIGHT() != nil {
		return "RIGHT"
	}
	if ctx.INNER_P() != nil {
		return "INNER"
	}
	return ""
}

// extractWhereClause appends WHERE predicates to the ParsedQuery.
func extractWhereClause(result *ParsedQuery, whereCtx gen.IWhere_clauseContext, tokens antlr.TokenStream) {
	if whereCtx == nil {
		return
	}
	if expr := whereCtx.A_expr(); expr != nil {
		prc, ok := expr.(antlr.ParserRuleContext)
		if !ok {
			return
		}
		clauseText := strings.TrimSpace(ctxText(tokens, prc))
		result.Where = append(result.Where, clauseText)
		extractExpressionSubqueries(result, expr, tokens)
		// Use the new comparison-aware extraction for WHERE clauses
		findAndRecordComparisons(result, expr, ColumnUsageTypeFilter, tokens)
	}
}

// extractHavingClause appends HAVING predicates to the ParsedQuery.
func extractHavingClause(result *ParsedQuery, havingCtx gen.IHaving_clauseContext, tokens antlr.TokenStream) {
	if havingCtx == nil {
		return
	}
	if expr := havingCtx.A_expr(); expr != nil {
		if prc, ok := expr.(antlr.ParserRuleContext); ok {
			result.Having = append(result.Having, strings.TrimSpace(ctxText(tokens, prc)))
		}
		extractExpressionSubqueries(result, expr, tokens)
		// Use the new comparison-aware extraction for HAVING clauses
		findAndRecordComparisons(result, expr, ColumnUsageTypeFilter, tokens)
	}
}

// extractGroupClause captures GROUP BY expressions.
func extractGroupClause(result *ParsedQuery, groupCtx gen.IGroup_clauseContext, tokens antlr.TokenStream) {
	if groupCtx == nil {
		return
	}
	list := groupCtx.Group_by_list()
	if list == nil {
		return
	}
	for _, item := range list.AllGroup_by_item() {
		if item == nil {
			continue
		}
		prc, ok := item.(antlr.ParserRuleContext)
		if !ok {
			continue
		}
		clauseText := strings.TrimSpace(ctxText(tokens, prc))
		result.GroupBy = append(result.GroupBy, clauseText)
		findAndRecordUsage(result, item, ColumnUsageTypeGroupBy, tokens)
	}
}

// extractOrderClause records ORDER BY expressions, direction, and NULLS handling.
func extractOrderClause(result *ParsedQuery, sortCtxWrap gen.ISort_clause_Context, tokens antlr.TokenStream) {
	if sortCtxWrap == nil {
		return
	}
	sortCtx := sortCtxWrap.Sort_clause()
	if sortCtx == nil {
		return
	}
	list := sortCtx.Sortby_list()
	if list == nil {
		return
	}
	for _, sortItem := range list.AllSortby() {
		item, ok := sortItem.(*gen.SortbyContext)
		if !ok || item == nil {
			continue
		}
		expr := ""
		dir := ""
		nulls := ""
		if item.A_expr() != nil {
			if prc, ok := item.A_expr().(antlr.ParserRuleContext); ok {
				expr = strings.TrimSpace(ctxText(tokens, prc))
			}
			findAndRecordUsage(result, item.A_expr(), ColumnUsageTypeOrderBy, tokens)
		}
		if item.Asc_desc_() != nil {
			if prc, ok := item.Asc_desc_().(antlr.ParserRuleContext); ok {
				dir = strings.TrimSpace(ctxText(tokens, prc))
			}
		}
		if item.Nulls_order_() != nil {
			if prc, ok := item.Nulls_order_().(antlr.ParserRuleContext); ok {
				nulls = strings.TrimSpace(ctxText(tokens, prc))
			}
		}
		if expr == "" && item.Qual_all_op() != nil {
			if prc, ok := item.Qual_all_op().(antlr.ParserRuleContext); ok {
				expr = strings.TrimSpace(ctxText(tokens, prc))
			}
		}
		result.OrderBy = append(result.OrderBy, OrderExpression{
			Expression: expr,
			Direction:  strings.ToUpper(dir),
			Nulls:      strings.ToUpper(nulls),
		})
	}
}

// extractLimitClause gathers LIMIT/OFFSET text from a SELECT.
func extractLimitClause(result *ParsedQuery, selectNoParens gen.ISelect_no_parensContext, tokens antlr.TokenStream, isNested bool) {
	if selectNoParens == nil {
		return
	}
	var limitText, offsetText string
	if limitCtx := selectNoParens.Select_limit(); limitCtx != nil {
		if limitClause := limitCtx.Limit_clause(); limitClause != nil {
			if prc, ok := limitClause.(antlr.ParserRuleContext); ok {
				limitText = strings.TrimSpace(ctxText(tokens, prc))
			}
		}
		if offsetClause := limitCtx.Offset_clause(); offsetClause != nil {
			if prc, ok := offsetClause.(antlr.ParserRuleContext); ok {
				offsetText = strings.TrimSpace(ctxText(tokens, prc))
			}
		}
	}
	if limitCtx := selectNoParens.Select_limit_(); limitCtx != nil && limitText == "" && offsetText == "" {
		if prc, ok := limitCtx.(antlr.ParserRuleContext); ok {
			limitText = strings.TrimSpace(ctxText(tokens, prc))
		}
	}
	if limitText != "" || offsetText != "" {
		result.Limit = &LimitClause{Limit: limitText, Offset: offsetText, IsNested: isNested}
	}
}

// detectLateralCorrelation attempts to detect correlations in LATERAL joins.
func detectLateralCorrelation(result *ParsedQuery, fnCtx gen.IFunc_tableContext, tokens antlr.TokenStream) {
	if result == nil || fnCtx == nil {
		return
	}

	// Get the function text to analyze for correlations
	prc, ok := fnCtx.(antlr.ParserRuleContext)
	if !ok {
		return
	}
	funcText := ctxText(tokens, prc)

	if result.Correlations == nil {
		result.Correlations = []JoinCorrelation{}
	}

	// Check if there are table aliases from the outer query referenced.
	// Use word-boundary matching to avoid false positives: short aliases like "a"
	// must not match substrings such as "data.".
	for _, table := range result.Tables {
		alias := table.Alias
		if alias == "" {
			alias = table.Name
		}
		if alias == "" {
			continue
		}
		// Check for alias followed by a dot with a word boundary before the alias.
		// This avoids regex compilation inside the loop.
		if !containsWordDot(funcText, alias) {
			continue
		}
		result.Correlations = append(result.Correlations, JoinCorrelation{
			OuterAlias: alias,
			InnerAlias: "", // LATERAL function doesn't have inner alias in this context
			Expression: funcText,
			Type:       "LATERAL",
		})
	}
}

// containsWordDot reports whether text contains word followed by a dot (e.g. "alias.")
// where word is preceded by a word boundary (start of string or a non-identifier character).
// This avoids regex compilation per call.
func containsWordDot(text, word string) bool {
	if word == "" {
		return false
	}
	needle := word + "."
	idx := 0
	for {
		pos := strings.Index(text[idx:], needle)
		if pos < 0 {
			return false
		}
		absPos := idx + pos
		// Check word boundary: character before must not be alphanumeric or underscore
		if absPos > 0 {
			prev := rune(text[absPos-1])
			if unicode.IsLetter(prev) || unicode.IsDigit(prev) || prev == '_' {
				idx = absPos + 1
				continue
			}
		}
		return true
	}
}
