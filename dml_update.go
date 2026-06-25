// dml_update.go implements UPDATE population logic for the IR builder.
package postgresparser

import (
	"fmt"

	"github.com/antlr4-go/antlr/v4"

	"github.com/valkdb/postgresparser/gen"
)

// populateUpdate records UPDATE targets, SET clauses, joins, and returning data.
func populateUpdate(result *ParsedQuery, ctx gen.IUpdatestmtContext, tokens antlr.TokenStream) error {
	if ctx == nil {
		return fmt.Errorf("update statement: %w", ErrNilContext)
	}

	if ctx.With_clause_() != nil {
		if withCtx := ctx.With_clause_().With_clause(); withCtx != nil {
			ctes, cteTables := extractCTEs(withCtx, tokens)
			result.CTEs = append(result.CTEs, ctes...)
			result.Tables = append(result.Tables, markNested(cteTables)...)
		}
	}
	cteNames := buildCTENameSet(result.CTEs)

	appendRelationOptAlias(result, ctx.Relation_expr_opt_alias(), tokens)
	if ctx.Set_clause_list() != nil {
		result.SetClauses = append(result.SetClauses, extractSetClauses(ctx.Set_clause_list(), tokens)...)
		recordSetTargetUsage(result, ctx.Set_clause_list(), ColumnUsageTypeDMLSet, tokens)
		findAndRecordUsage(result, ctx.Set_clause_list(), ColumnUsageTypeDMLSet, tokens)
	}
	if ctx.From_clause() != nil {
		extractFromClause(result, ctx.From_clause(), tokens, cteNames)
	}
	if where := ctx.Where_or_current_clause(); where != nil {
		if prc, ok := where.(antlr.ParserRuleContext); ok {
			appendWhereClause(result, prc, tokens)
		}
	}
	appendReturningClause(result, ctx.Returning_clause(), tokens)
	return nil
}
