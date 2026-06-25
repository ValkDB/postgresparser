// dml_delete.go implements DELETE population logic for the IR builder.
package postgresparser

import (
	"fmt"

	"github.com/antlr4-go/antlr/v4"

	"github.com/valkdb/postgresparser/gen"
)

// populateDelete captures DELETE targets, USING tables, predicates, and returning data.
func populateDelete(result *ParsedQuery, ctx gen.IDeletestmtContext, tokens antlr.TokenStream) error {
	if ctx == nil {
		return fmt.Errorf("delete statement: %w", ErrNilContext)
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
	if ctx.Using_clause() != nil {
		extractFromList(result, ctx.Using_clause().From_list(), tokens, cteNames)
	}
	if where := ctx.Where_or_current_clause(); where != nil {
		if prc, ok := where.(antlr.ParserRuleContext); ok {
			appendWhereClause(result, prc, tokens)
		}
	}
	appendReturningClause(result, ctx.Returning_clause(), tokens)
	return nil
}
