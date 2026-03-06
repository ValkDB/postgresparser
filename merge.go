// merge.go extracts metadata for MERGE statements including action clauses.
package postgresparser

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"

	"github.com/valkdb/postgresparser/gen"
)

// populateMerge builds MergeClause metadata for target, source, and action clauses.
func populateMerge(result *ParsedQuery, ctx gen.IMergestmtContext, tokens antlr.TokenStream) error {
	if result == nil {
		return fmt.Errorf("merge result container: %w", ErrNilContext)
	}
	if ctx == nil {
		return fmt.Errorf("merge statement: %w", ErrNilContext)
	}

	merge := &MergeClause{}
	aliasClauses := ctx.AllAlias_clause()
	aliasIndex := 0

	var targetAlias string
	if len(aliasClauses) > 0 {
		targetAlias = aliasFromAliasClause(aliasClauses[aliasIndex], tokens)
		aliasIndex++
	}

	qualifiedNames := ctx.AllQualified_name()
	if len(qualifiedNames) > 0 {
		targetName := ""
		if prc, ok := qualifiedNames[0].(antlr.ParserRuleContext); ok {
			targetName = strings.TrimSpace(ctxText(tokens, prc))
		}
		schema, relation := splitQualifiedName(targetName)
		merge.Target = TableRef{
			Schema: schema,
			Name:   relation,
			Alias:  targetAlias,
			Type:   TableTypeBase,
			Raw:    targetName,
		}
		appendSetOpTables(result, nil, []TableRef{merge.Target})
		if rc, ok := qualifiedNames[0].(antlr.RuleContext); ok {
			findAndRecordUsage(result, rc, ColumnUsageTypeMergeTarget, tokens)
		}
	}

	var sourceAlias string
	if len(aliasClauses) > aliasIndex {
		sourceAlias = aliasFromAliasClause(aliasClauses[aliasIndex], tokens)
	}

	if swp := ctx.Select_with_parens(); swp != nil {
		raw := ""
		if prc, ok := swp.(antlr.ParserRuleContext); ok {
			raw = strings.TrimSpace(ctxText(tokens, prc))
		}
		merge.Source.Table = TableRef{
			Name:  sourceAlias,
			Alias: sourceAlias,
			Type:  TableTypeSubquery,
			Raw:   raw,
		}
		appendSetOpTables(result, nil, []TableRef{merge.Source.Table})
		// Build nested subquery analysis without flattening inner column usage
		// into the MERGE parent scope.
		if subRef, err := buildSubqueryRef(sourceAlias, swp, tokens); err == nil && subRef != nil {
			merge.Source.Subquery = subRef
			result.Subqueries = append(result.Subqueries, *subRef)
			appendSetOpTables(result, nil, subRef.Query.Tables)
		}
	} else if len(qualifiedNames) > 1 {
		sourceName := ""
		if prc, ok := qualifiedNames[1].(antlr.ParserRuleContext); ok {
			sourceName = strings.TrimSpace(ctxText(tokens, prc))
		}
		schema, relation := splitQualifiedName(sourceName)
		merge.Source.Table = TableRef{
			Schema: schema,
			Name:   relation,
			Alias:  sourceAlias,
			Type:   TableTypeBase,
			Raw:    sourceName,
		}
		appendSetOpTables(result, nil, []TableRef{merge.Source.Table})
		if rc, ok := qualifiedNames[1].(antlr.RuleContext); ok {
			findAndRecordUsage(result, rc, ColumnUsageTypeMergeSource, tokens)
		}
	}

	if cond := ctx.A_expr(); cond != nil {
		if prc, ok := cond.(antlr.ParserRuleContext); ok {
			merge.Condition = strings.TrimSpace(ctxText(tokens, prc))
		}
		findAndRecordUsage(result, cond, ColumnUsageTypeJoin, tokens)
	}

	for i := 0; i < ctx.GetChildCount(); i++ {
		switch node := ctx.GetChild(i).(type) {
		case *gen.Merge_insert_clauseContext:
			action := buildMergeInsertAction(result, node, tokens)
			if action.Type != "" {
				merge.Actions = append(merge.Actions, action)
			}
		case *gen.Merge_update_clauseContext:
			action := buildMergeUpdateAction(result, node, tokens)
			if action.Type != "" {
				merge.Actions = append(merge.Actions, action)
				if len(action.SetClauses) > 0 {
					result.SetClauses = append(result.SetClauses, action.SetClauses...)
				}
			}
		case *gen.Merge_delete_clauseContext:
			action := buildMergeDeleteAction(result, node, tokens)
			if action.Type != "" {
				merge.Actions = append(merge.Actions, action)
			}
		}
	}

	result.Merge = merge
	return nil
}

// buildMergeInsertAction captures INSERT clauses from WHEN NOT MATCHED branches.
func buildMergeInsertAction(result *ParsedQuery, ctx *gen.Merge_insert_clauseContext, tokens antlr.TokenStream) MergeAction {
	if ctx == nil {
		return MergeAction{}
	}
	action := MergeAction{Type: "INSERT"}
	if cond := ctx.A_expr(); cond != nil {
		if prc, ok := cond.(antlr.ParserRuleContext); ok {
			action.Condition = strings.TrimSpace(ctxText(tokens, prc))
		}
		findAndRecordUsage(result, cond, ColumnUsageTypeFilter, tokens)
	}
	if cols := ctx.Insert_column_list(); cols != nil {
		action.InsertColumns = extractInsertColumns(cols, tokens)
	}
	if values := ctx.Values_clause(); values != nil {
		if prc, ok := values.(antlr.ParserRuleContext); ok {
			action.InsertValues = strings.TrimSpace(ctxText(tokens, prc))
		}
		findAndRecordUsage(result, values, ColumnUsageTypeMergeInsert, tokens)
	}
	return action
}

// buildMergeUpdateAction captures UPDATE clauses from WHEN MATCHED branches.
func buildMergeUpdateAction(result *ParsedQuery, ctx *gen.Merge_update_clauseContext, tokens antlr.TokenStream) MergeAction {
	if ctx == nil {
		return MergeAction{}
	}
	action := MergeAction{Type: "UPDATE"}
	if cond := ctx.A_expr(); cond != nil {
		if prc, ok := cond.(antlr.ParserRuleContext); ok {
			action.Condition = strings.TrimSpace(ctxText(tokens, prc))
		}
		findAndRecordUsage(result, cond, ColumnUsageTypeFilter, tokens)
	}
	if scl := ctx.Set_clause_list(); scl != nil {
		action.SetClauses = extractSetClauses(scl, tokens)
		recordSetTargetUsage(result, scl, ColumnUsageTypeMergeSet, tokens)
		findAndRecordUsage(result, scl, ColumnUsageTypeMergeSet, tokens)
	}
	return action
}

// buildMergeDeleteAction records DELETE clauses from WHEN MATCHED branches.
// The grammar has been updated to support (AND a_expr)? but until gen/ is regenerated,
// we extract the condition from the clause's token text as a workaround.
// Once regenerated, this should use ctx.A_expr() like the insert/update actions.
func buildMergeDeleteAction(result *ParsedQuery, ctx *gen.Merge_delete_clauseContext, tokens antlr.TokenStream) MergeAction {
	if ctx == nil {
		return MergeAction{}
	}
	action := MergeAction{Type: "DELETE"}

	// Extract condition from the clause text.
	// The pattern is: WHEN MATCHED [AND <condition>] [THEN] DELETE
	// Since the generated parser does not yet expose A_expr() for delete clauses,
	// we extract the condition from the raw text between "AND" and "THEN"/"DELETE".
	clauseText := ctxText(tokens, ctx)
	action.Condition = extractDeleteConditionFromText(clauseText)
	if action.Condition != "" && result != nil {
		// Record the condition text for column usage tracking.
		// Full AST-based recording will be possible once gen/ is regenerated.
		_ = result // Placeholder for findAndRecordUsage once A_expr is available
	}

	return action
}

// extractDeleteConditionFromText extracts the AND condition from a MERGE delete
// clause text like "WHEN MATCHED AND <expr> THEN DELETE" or "WHEN MATCHED AND <expr> DELETE".
// Returns empty string if no AND condition is present.
func extractDeleteConditionFromText(text string) string {
	upper := strings.ToUpper(text)
	andIdx := strings.Index(upper, " AND ")
	if andIdx < 0 {
		return ""
	}
	after := text[andIdx+5:] // skip " AND "

	// Find where the condition ends: at " THEN " or " DELETE" (whichever comes first)
	upperAfter := strings.ToUpper(after)
	endIdx := len(after)
	if idx := strings.Index(upperAfter, " THEN"); idx >= 0 && idx < endIdx {
		endIdx = idx
	}
	if idx := strings.Index(upperAfter, " DELETE"); idx >= 0 && idx < endIdx {
		endIdx = idx
	}

	return strings.TrimSpace(after[:endIdx])
}
