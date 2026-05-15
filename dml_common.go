// dml_common.go hosts helpers shared by INSERT/UPDATE/DELETE/MERGE builders.
package postgresparser

import (
	"strings"

	"github.com/antlr4-go/antlr/v4"

	"github.com/valkdb/postgresparser/gen"
)

// extractInsertColumns converts an insert_column_list into raw column names.
func extractInsertColumns(list gen.IInsert_column_listContext, tokens antlr.TokenStream) []string {
	if list == nil {
		return nil
	}
	items := list.AllInsert_column_item()
	cols := make([]string, 0, len(items))
	for _, item := range items {
		prc, ok := item.(antlr.ParserRuleContext)
		if !ok {
			continue
		}
		text := strings.TrimSpace(ctxText(tokens, prc))
		if text != "" {
			cols = append(cols, text)
		}
	}
	return cols
}

// extractSetClauses returns raw text for each SET clause encountered.
func extractSetClauses(list gen.ISet_clause_listContext, tokens antlr.TokenStream) []string {
	if list == nil {
		return nil
	}
	var clauses []string
	for _, clause := range list.AllSet_clause() {
		prc, ok := clause.(antlr.ParserRuleContext)
		if !ok {
			continue
		}
		text := strings.TrimSpace(ctxText(tokens, prc))
		if text != "" {
			clauses = append(clauses, text)
		}
	}
	return clauses
}

// recordSetTargetUsage registers target identifiers for SET clauses with the given usage role.
func recordSetTargetUsage(result *ParsedQuery, list gen.ISet_clause_listContext, role ColumnUsageType, tokens antlr.TokenStream) {
	if result == nil || list == nil {
		return
	}
	for _, clause := range list.AllSet_clause() {
		setCtx, ok := clause.(*gen.Set_clauseContext)
		if !ok || setCtx == nil {
			continue
		}
		target := setCtx.Set_target()
		if target == nil {
			continue
		}
		parts := []string{}
		if target.Colid() != nil {
			parts = append(parts, strings.TrimSpace(target.Colid().GetText()))
		}
		if opt := target.Opt_indirection(); opt != nil {
			for _, el := range opt.AllIndirection_el() {
				if indCtx, ok := el.(*gen.Indirection_elContext); ok {
					if indCtx.Attr_name() != nil {
						parts = append(parts, strings.TrimSpace(indCtx.Attr_name().GetText()))
					}
				}
			}
		}
		ref := columnRefFromParts(parts)
		if ref.Name == "" {
			continue
		}
		expression := text(tokens, target)
		result.ColumnUsage = append(result.ColumnUsage, ColumnUsage{
			TableAlias: ref.TableAlias,
			Column:     ref.Name,
			Expression: expression,
			UsageType:  role,
			Context:    strings.TrimSpace(ctxText(tokens, setCtx)),
		})
	}
}

// appendReturningClause records RETURNING clauses and finds column usages.
func appendReturningClause(result *ParsedQuery, returning gen.IReturning_clauseContext, tokens antlr.TokenStream) {
	if returning == nil {
		return
	}
	if returningText := text(tokens, returning); returningText != "" {
		result.Returning = append(result.Returning, returningText)
	}
	findAndRecordUsage(result, returning, ColumnUsageTypeReturning, tokens)
}

// appendRelationOptAlias registers the main relation referenced by an UPDATE/DELETE.
func appendRelationOptAlias(result *ParsedQuery, rel gen.IRelation_expr_opt_aliasContext, tokens antlr.TokenStream) {
	if rel == nil {
		return
	}
	if rel.Relation_expr() == nil {
		return
	}
	nameText := text(tokens, rel.Relation_expr())
	schema, name := splitQualifiedName(nameText)
	alias := ""
	if rel.Colid() != nil {
		alias = text(tokens, rel.Colid())
	}
	result.Tables = append(result.Tables, TableRef{
		Schema: schema,
		Name:   name,
		Alias:  alias,
		Type:   TableTypeBase,
		Raw:    nameText,
	})
}

// appendWhereClause adds WHERE/CURRENT clause text and finds column usages.
func appendWhereClause(result *ParsedQuery, whereCtx antlr.ParserRuleContext, tokens antlr.TokenStream) {
	if whereCtx == nil {
		return
	}
	text := strings.TrimSpace(ctxText(tokens, whereCtx))
	if text != "" {
		result.Where = append(result.Where, text)
	}
	// DML WHERE: enable wrapper extraction.
	findAndRecordComparisons(result, whereCtx, ColumnUsageTypeFilter, tokens, true)
}

// buildCTENameSet creates a lookup used to classify table references against WITH bindings.
func buildCTENameSet(ctes []CTE) map[string]struct{} {
	set := make(map[string]struct{}, len(ctes))
	for _, cte := range ctes {
		if cte.Name != "" {
			set[strings.ToLower(cte.Name)] = struct{}{}
		}
	}
	return set
}
