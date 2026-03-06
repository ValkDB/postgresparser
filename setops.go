// setops.go hydrates UNION/INTERSECT/EXCEPT metadata and shared subquery helpers.
package postgresparser

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"

	"github.com/valkdb/postgresparser/gen"
)

// extractSetOperationsWithResult traverses UNION/INTERSECT/EXCEPT chains with optional result for column usage recording.
func extractSetOperationsWithResult(selectNoParens gen.ISelect_no_parensContext, tokens antlr.TokenStream, cteNames map[string]struct{}, result *ParsedQuery) ([]SetOperation, []TableRef, []SubqueryRef) {
	if selectNoParens == nil {
		return nil, nil, nil
	}
	selectClause := selectNoParens.Select_clause()
	if selectClause == nil {
		return nil, nil, nil
	}
	clauseCtx, ok := selectClause.(*gen.Select_clauseContext)
	if !ok || clauseCtx == nil {
		return nil, nil, nil
	}

	var ops []SetOperation
	var leading []TableRef
	var subqueries []SubqueryRef

	// Check if there are actual set operations (UNION, EXCEPT, INTERSECT)
	hasSetOps := false
	childCount := clauseCtx.GetChildCount()
	for i := 0; i < childCount; i++ {
		child := clauseCtx.GetChild(i)
		if term, ok := child.(antlr.TerminalNode); ok {
			opToken := strings.ToUpper(strings.TrimSpace(term.GetText()))
			if opToken == "UNION" || opToken == "EXCEPT" {
				hasSetOps = true
				break
			}
		}
	}

	// Also check for INTERSECT operations
	if !hasSetOps {
		if first := clauseCtx.Simple_select_intersect(0); first != nil {
			ctx, ok := first.(*gen.Simple_select_intersectContext)
			if ok && ctx != nil {
				cc := ctx.GetChildCount()
				for i := 0; i < cc; i++ {
					if term, ok := ctx.GetChild(i).(antlr.TerminalNode); ok {
						if strings.ToUpper(strings.TrimSpace(term.GetText())) == "INTERSECT" {
							hasSetOps = true
							break
						}
					}
				}
			}
		}
	}

	if first := clauseCtx.Simple_select_intersect(0); first != nil {
		// Only process the first primary if there are actual set operations
		// Otherwise it was already processed by extractWhereClause in the main SELECT
		if hasSetOps {
			if primary := first.Simple_select_pramary(0); primary != nil {
				// Pass result through to capture column usage from first SELECT
				firstTables, firstSubs := extractTablesAndUsageForPrimary(primary, tokens, cteNames, result)
				leading = append(leading, firstTables...)
				subqueries = append(subqueries, firstSubs...)
			}
		}
		nestedOps, nestedLeading, nestedSubs := collectIntersectOperationsWithResult(first, tokens, cteNames, result)
		ops = append(ops, nestedOps...)
		leading = append(leading, nestedLeading...)
		subqueries = append(subqueries, nestedSubs...)
	}

	// Process UNION and EXCEPT operations
	for i := 0; i < childCount; i++ {
		child := clauseCtx.GetChild(i)
		term, ok := child.(antlr.TerminalNode)
		if !ok {
			continue
		}
		opToken := strings.ToUpper(strings.TrimSpace(term.GetText()))
		if opToken != "UNION" && opToken != "EXCEPT" {
			continue
		}

		opType := opToken
		next := i + 1
		if next < childCount {
			if modifierCtx, ok := clauseCtx.GetChild(next).(gen.IAll_or_distinctContext); ok {
				if prc, ok2 := modifierCtx.(antlr.ParserRuleContext); ok2 {
					modifier := strings.ToUpper(strings.TrimSpace(ctxText(tokens, prc)))
					if modifier == "ALL" {
						opType += " ALL"
					}
				}
				next++
			}
		}

		if next >= childCount {
			continue
		}
		rhs, ok := clauseCtx.GetChild(next).(gen.ISimple_select_intersectContext)
		if !ok || rhs == nil {
			continue
		}

		op, opSubs := buildSetOperationFromIntersect(opType, rhs, tokens, cteNames, result)
		ops = append(ops, op)
		subqueries = append(subqueries, opSubs...)
		nestedOps, nestedLeading, nestedSubs := collectIntersectOperationsWithResult(rhs, tokens, cteNames, result)
		ops = append(ops, nestedOps...)
		leading = append(leading, nestedLeading...)
		subqueries = append(subqueries, nestedSubs...)

		i = next
	}

	return ops, leading, subqueries
}

// collectIntersectOperationsWithResult inspects INTERSECT branches with optional result for column usage.
func collectIntersectOperationsWithResult(node gen.ISimple_select_intersectContext, tokens antlr.TokenStream, cteNames map[string]struct{}, result *ParsedQuery) ([]SetOperation, []TableRef, []SubqueryRef) {
	ctx, ok := node.(*gen.Simple_select_intersectContext)
	if !ok || ctx == nil {
		return nil, nil, nil
	}
	var ops []SetOperation
	var leading []TableRef
	var subqueries []SubqueryRef

	if primary := ctx.Simple_select_pramary(0); primary != nil {
		nestedOps, nestedLeading, nestedSubs := collectNestedSetOperationsFromPrimary(primary, tokens, cteNames)
		ops = append(ops, nestedOps...)
		leading = append(leading, nestedLeading...)
		subqueries = append(subqueries, nestedSubs...)
	}

	childCount := ctx.GetChildCount()
	for i := 0; i < childCount; i++ {
		child := ctx.GetChild(i)
		term, ok := child.(antlr.TerminalNode)
		if !ok {
			continue
		}
		if strings.ToUpper(strings.TrimSpace(term.GetText())) != "INTERSECT" {
			continue
		}
		opType := "INTERSECT"
		next := i + 1
		if next < childCount {
			if modifierCtx, ok := ctx.GetChild(next).(gen.IAll_or_distinctContext); ok {
				if prc, ok2 := modifierCtx.(antlr.ParserRuleContext); ok2 {
					modifier := strings.ToUpper(strings.TrimSpace(ctxText(tokens, prc)))
					if modifier == "ALL" {
						opType += " ALL"
					}
				}
				next++
			}
		}
		if next >= childCount {
			continue
		}
		primary, ok := ctx.GetChild(next).(gen.ISimple_select_pramaryContext)
		if !ok || primary == nil {
			continue
		}

		op, opSubs := buildSetOperationFromPrimary(opType, primary, tokens, cteNames)
		ops = append(ops, op)
		subqueries = append(subqueries, opSubs...)
		nestedOps, nestedLeading, nestedSubs := collectNestedSetOperationsFromPrimary(primary, tokens, cteNames)
		ops = append(ops, nestedOps...)
		leading = append(leading, nestedLeading...)
		subqueries = append(subqueries, nestedSubs...)
		i = next
	}

	return ops, leading, subqueries
}

// buildSetOperationFromIntersect materialises metadata for a UNION/EXCEPT right-hand SELECT.
func buildSetOperationFromIntersect(opType string, rhs gen.ISimple_select_intersectContext, tokens antlr.TokenStream, cteNames map[string]struct{}, result *ParsedQuery) (SetOperation, []SubqueryRef) {
	query := ""
	if prc, ok := rhs.(antlr.ParserRuleContext); ok {
		query = strings.TrimSpace(ctxText(tokens, prc))
	}
	op := SetOperation{
		Type:  strings.TrimSpace(opType),
		Query: query,
	}
	var subqueries []SubqueryRef
	if primary := rhs.Simple_select_pramary(0); primary != nil {
		op.Columns = extractColumnsForPrimary(primary, tokens)
		// Pass result through to capture column usage
		tables, subs := extractTablesAndUsageForPrimary(primary, tokens, cteNames, result)
		op.Tables = append(op.Tables, tables...)
		subqueries = append(subqueries, subs...)
	}
	return op, subqueries
}

// buildSetOperationFromPrimary materialises metadata for an INTERSECT primary SELECT.
func buildSetOperationFromPrimary(opType string, primary gen.ISimple_select_pramaryContext, tokens antlr.TokenStream, cteNames map[string]struct{}) (SetOperation, []SubqueryRef) {
	tables, subqueries := extractTablesForPrimary(primary, tokens, cteNames)
	query := ""
	if prc, ok := primary.(antlr.ParserRuleContext); ok {
		query = strings.TrimSpace(ctxText(tokens, prc))
	}
	return SetOperation{
		Type:    strings.TrimSpace(opType),
		Query:   query,
		Columns: extractColumnsForPrimary(primary, tokens),
		Tables:  tables,
	}, subqueries
}

// extractColumnsForPrimary builds a projection list for a set-operation SELECT primary.
func extractColumnsForPrimary(primary gen.ISimple_select_pramaryContext, tokens antlr.TokenStream) []string {
	if primary == nil {
		return nil
	}
	tmp := &ParsedQuery{DerivedColumns: make(map[string]string)}
	extractProjection(tmp, primary, tokens)
	if len(tmp.Columns) == 0 {
		return nil
	}
	cols := make([]string, len(tmp.Columns))
	for i, col := range tmp.Columns {
		cols[i] = col.Expression
	}
	return cols
}

// collectNestedSetOperationsFromPrimary dives into nested parenthesised selects for more set operations.
func collectNestedSetOperationsFromPrimary(primary gen.ISimple_select_pramaryContext, tokens antlr.TokenStream, cteNames map[string]struct{}) ([]SetOperation, []TableRef, []SubqueryRef) {
	if primary == nil {
		return nil, nil, nil
	}
	swp := primary.Select_with_parens()
	if swp == nil {
		return nil, nil, nil
	}
	_, _, inner, err := resolveSelectFromParens(swp)
	if err != nil || inner == nil {
		return nil, nil, nil
	}
	return extractSetOperationsWithResult(inner, tokens, cteNames, nil)
}

// extractTablesForPrimary recovers table references and nested subqueries from a select primary.
func extractTablesForPrimary(primary gen.ISimple_select_pramaryContext, tokens antlr.TokenStream, cteNames map[string]struct{}) ([]TableRef, []SubqueryRef) {
	return extractTablesAndUsageForPrimary(primary, tokens, cteNames, nil)
}

// extractTablesAndUsageForPrimary recovers table references, subqueries, and column usage from a select primary.
func extractTablesAndUsageForPrimary(primary gen.ISimple_select_pramaryContext, tokens antlr.TokenStream, cteNames map[string]struct{}, result *ParsedQuery) ([]TableRef, []SubqueryRef) {
	if primary == nil {
		return nil, nil
	}
	tmp := &ParsedQuery{}
	if from := primary.From_clause(); from != nil {
		extractFromClause(tmp, from, tokens, cteNames)
	}
	// FIX: Extract WHERE clause filters for UNION/EXCEPT RHS queries
	// If result is provided, record column usage there; otherwise use tmp
	targetResult := tmp
	if result != nil {
		targetResult = result
	}
	if where := primary.Where_clause(); where != nil {
		// Use the new comparison-aware extraction for WHERE clauses
		if whereExpr := where.A_expr(); whereExpr != nil {
			findAndRecordComparisons(targetResult, whereExpr, ColumnUsageTypeFilter, tokens)
		}
	}
	if primary.TABLE() != nil && primary.Relation_expr() != nil {
		name := ""
		if prc, ok := primary.Relation_expr().(antlr.ParserRuleContext); ok {
			name = strings.TrimSpace(ctxText(tokens, prc))
		}
		schema, relation := splitQualifiedName(name)
		tableType := TableTypeBase
		if _, ok := cteNames[strings.ToLower(relation)]; ok {
			tableType = TableTypeCTE
		}
		tmp.Tables = append(tmp.Tables, TableRef{
			Schema: schema,
			Name:   relation,
			Type:   tableType,
			Raw:    name,
		})
	}
	if primary.Select_with_parens() != nil {
		// Build nested subquery analysis without flattening inner column usage
		// into the parent query scope.
		if subRef, err := buildSubqueryRef("", primary.Select_with_parens(), tokens); err == nil && subRef != nil {
			tmp.Subqueries = append(tmp.Subqueries, *subRef)
			tmp.Tables = append(tmp.Tables, subRef.Query.Tables...)
		}
	}
	return tmp.Tables, tmp.Subqueries
}

// buildSubqueryRef parses a parenthesised subquery into nested IR.
func buildSubqueryRef(alias string, selectWithParens gen.ISelect_with_parensContext, tokens antlr.TokenStream) (*SubqueryRef, error) {
	if selectWithParens == nil {
		return nil, nil
	}
	withClause, simple, selectNoParens, err := resolveSelectFromParens(selectWithParens)
	if err != nil {
		return nil, err
	}
	if simple == nil || selectNoParens == nil {
		return nil, fmt.Errorf("unable to resolve subquery select")
	}
	rawSQL := ""
	if prc, ok := selectWithParens.(antlr.ParserRuleContext); ok {
		rawSQL = strings.TrimSpace(ctxText(tokens, prc))
	}
	parsed := &ParsedQuery{
		Command:        QueryCommandSelect,
		RawSQL:         rawSQL,
		DerivedColumns: make(map[string]string),
	}
	if err := populateSelectFromResolvedNested(parsed, withClause, simple, selectNoParens, tokens, true); err != nil {
		return nil, err
	}

	return &SubqueryRef{
		Alias: alias,
		Query: parsed,
	}, nil
}

// appendSetOpTables merges deduplicated tables discovered via set operations into the parent query.
func appendSetOpTables(result *ParsedQuery, ops []SetOperation, leading []TableRef) {
	if result == nil {
		return
	}

	seen := make(map[string]struct{}, len(result.Tables))
	for _, tbl := range result.Tables {
		seen[tableKey(tbl)] = struct{}{}
	}

	merge := func(tables []TableRef) {
		for _, tbl := range tables {
			key := tableKey(tbl)
			if _, ok := seen[key]; ok {
				continue
			}
			result.Tables = append(result.Tables, tbl)
			seen[key] = struct{}{}
		}
	}

	merge(leading)
	for _, op := range ops {
		merge(op.Tables)
	}
}

// tableKey generates a case-insensitive key used to deduplicate table references.
func tableKey(t TableRef) string {
	return strings.ToLower(fmt.Sprintf("%s.%s|%s|%s", t.Schema, t.Name, t.Alias, t.Type))
}
