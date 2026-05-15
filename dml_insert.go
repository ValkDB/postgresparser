// dml_insert.go implements INSERT population including ON CONFLICT metadata.
package postgresparser

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"

	"github.com/valkdb/postgresparser/gen"
)

// populateInsert records INSERT target tables, columns, and upsert metadata.
func populateInsert(result *ParsedQuery, ctx gen.IInsertstmtContext, tokens antlr.TokenStream) error {
	if ctx == nil {
		return fmt.Errorf("insert statement: %w", ErrNilContext)
	}

	if ctx.With_clause_() != nil {
		if withCtx := ctx.With_clause_().With_clause(); withCtx != nil {
			ctes, cteTables := extractCTEs(withCtx, tokens)
			result.CTEs = append(result.CTEs, ctes...)
			result.Tables = append(result.Tables, cteTables...)
		}
	}
	if target := ctx.Insert_target(); target != nil {
		qn := target.Qualified_name()
		if qn != nil {
			nameText := ""
			nameText = text(tokens, qn)
			schema, name := splitQualifiedName(nameText)
			alias := ""
			if target.Colid() != nil {
				alias = text(tokens, target.Colid())
			}
			tbl := TableRef{
				Schema: schema,
				Name:   name,
				Alias:  alias,
				Type:   TableTypeBase,
				Raw:    nameText,
			}
			result.Tables = append(result.Tables, tbl)
		}
	}

	if rest := ctx.Insert_rest(); rest != nil {
		if cols := rest.Insert_column_list(); cols != nil {
			result.InsertColumns = append(result.InsertColumns, extractInsertColumns(cols, tokens)...)
		}
		if conflictSet := ctx.On_conflict_(); conflictSet != nil {
			if upsert := buildUpsertClause(result, conflictSet, tokens); upsert != nil {
				result.Upsert = upsert
				if len(upsert.SetClauses) > 0 {
					result.SetClauses = append(result.SetClauses, upsert.SetClauses...)
				}
			}
		}
		if rest.Selectstmt() != nil {
			if err := populateSelect(result, rest.Selectstmt(), tokens); err != nil {
				return err
			}
		}
	}

	appendReturningClause(result, ctx.Returning_clause(), tokens)
	return nil
}

// buildUpsertClause structures ON CONFLICT metadata including target and action.
func buildUpsertClause(result *ParsedQuery, conflict gen.IOn_conflict_Context, tokens antlr.TokenStream) *UpsertClause {
	if conflict == nil {
		return nil
	}
	upsert := &UpsertClause{}
	if confExpr := conflict.Conf_expr_(); confExpr != nil {
		if params := confExpr.Index_params(); params != nil {
			for _, elem := range params.AllIndex_elem() {
				prc, ok := elem.(antlr.ParserRuleContext)
				if !ok {
					continue
				}
				text := strings.TrimSpace(ctxText(tokens, prc))
				if text != "" {
					upsert.TargetColumns = append(upsert.TargetColumns, text)
				}
			}
			findAndRecordUsage(result, params, ColumnUsageTypeUpsertTarget, tokens)
		}
		if confExpr.Where_clause() != nil {
			if targetWhere := text(tokens, confExpr.Where_clause()); targetWhere != "" {
				upsert.TargetWhere = targetWhere
				findAndRecordUsage(result, confExpr.Where_clause(), ColumnUsageTypeFilter, tokens)
			}
		}
		if confExpr.Name() != nil {
			upsert.Constraint = text(tokens, confExpr.Name())
		}
	}
	switch {
	case conflict.NOTHING() != nil:
		upsert.Action = "DO NOTHING"
	case conflict.UPDATE() != nil:
		upsert.Action = "DO UPDATE"
		if scl := conflict.Set_clause_list(); scl != nil {
			upsert.SetClauses = extractSetClauses(scl, tokens)
			recordSetTargetUsage(result, scl, ColumnUsageTypeDMLSet, tokens)
			findAndRecordUsage(result, scl, ColumnUsageTypeUpsertSet, tokens)
		}
		if where := conflict.Where_clause(); where != nil {
			if actionWhere := text(tokens, where); actionWhere != "" {
				upsert.ActionWhere = actionWhere
				findAndRecordUsage(result, where, ColumnUsageTypeFilter, tokens)
			}
		}
	default:
		upsert.Action = "DO NOTHING"
	}
	return upsert
}
