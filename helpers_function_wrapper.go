// helpers_function_wrapper.go detects allowlisted function wrappers around bare
// column references in WHERE-clause predicates and produces FunctionWrapper
// metadata for ColumnUsage. Detection is structural (AST-driven), not textual,
// so it survives nested calls, casts, parentheses, and quoted column names.
package postgresparser

import (
	"strings"

	"github.com/antlr4-go/antlr/v4"

	"github.com/valkdb/postgresparser/gen"
)

// allowedWrapperFunctions is the canonical lowercase set of function names that
// may produce a FunctionWrapper on a WHERE-clause predicate. The set is the
// structural-semantic boundary for "function calls that wrap a single column in
// a predicate position and change the comparison's semantics" — any wider set
// would dilute the contract for downstream consumers (ORMs, linters, rewriters,
// AI-assisted SQL generators) that key off it.
var allowedWrapperFunctions = map[string]struct{}{
	"length":       {},
	"lower":        {},
	"upper":        {},
	"coalesce":     {},
	"extract":      {},
	"date_trunc":   {},
	"char_length":  {},
	"octet_length": {},
}

// detectWrappersInComparison walks the immediate operand expressions of a
// comparison node and returns any allowlisted function wrappers attached to
// bare columns, keyed by tableAlias|column (matching the dedup key used by
// findAndRecordComparisons). Used only when wantWrappers is true at the call
// site.
//
// The compare-rule shape varies: A_expr_isnull / A_expr_is_not interpose extra
// rule levels that carry IS/NOT/NULL keyword terminals between the comparison
// node and its actual operand expression. This walker peels those wrappers so
// detection sees the operand A_expr ladder.
func detectWrappersInComparison(ctx antlr.ParserRuleContext) map[string]*FunctionWrapper {
	if ctx == nil {
		return nil
	}
	out := map[string]*FunctionWrapper{}
	collectOperandExprs(ctx, func(operand antlr.ParserRuleContext) {
		wrapper, col, ok := detectFunctionWrapperOnExpr(operand)
		if !ok {
			return
		}
		key := col.TableAlias + "|" + col.Name
		if _, exists := out[key]; !exists {
			out[key] = wrapper
		}
	})
	return out
}

// collectOperandExprs invokes visit on each operand expression of a comparison
// node. For most comparison kinds the operands are direct rule children. For
// A_expr_isnull / A_expr_is_not the operand sits one rule level deeper because
// IS/NOT/NULL/etc. keywords are siblings to the operand expr.
func collectOperandExprs(ctx antlr.ParserRuleContext, visit func(antlr.ParserRuleContext)) {
	switch n := ctx.(type) {
	case *gen.A_expr_isnullContext:
		// Recurse one level into A_expr_is_not; it is itself a single-child
		// wrapper for the operand A_expr_compare ladder.
		if n.A_expr_is_not() != nil {
			collectOperandExprs(n.A_expr_is_not().(antlr.ParserRuleContext), visit)
		}
		return
	case *gen.A_expr_is_notContext:
		if n.A_expr_compare() != nil {
			visit(n.A_expr_compare().(antlr.ParserRuleContext))
		}
		return
	}
	for i := 0; i < ctx.GetChildCount(); i++ {
		if rc, ok := ctx.GetChild(i).(antlr.ParserRuleContext); ok {
			visit(rc)
		}
	}
}

// detectFunctionWrapperOnExpr tries to interpret expr as a (possibly
// cast-wrapped, possibly nested) allowlisted function call around a bare
// column reference. Returns the wrapper plus the bare column. ok=false for
// anything else (literal, raw column, expression, non-allowlisted call, ...).
func detectFunctionWrapperOnExpr(expr antlr.ParserRuleContext) (*FunctionWrapper, columnRef, bool) {
	cast, funcCtx := descendToWrapperRoot(expr)
	if funcCtx == nil {
		return nil, columnRef{}, false
	}
	name, schema, args, columnExpr, ok := decodeAllowlistedCall(funcCtx)
	if !ok || columnExpr == nil {
		return nil, columnRef{}, false
	}
	if nestedW, nestedCol, nestedOK := detectFunctionWrapperOnExpr(columnExpr); nestedOK {
		_ = nestedW // inner wrapper is collapsed; outermost name wins per spec
		return &FunctionWrapper{
			Name:     name,
			Schema:   schema,
			Args:     args,
			IsNested: true,
			Cast:     cast,
		}, nestedCol, true
	}
	col, isBare := bareColumnFromExpr(columnExpr)
	if !isBare {
		return nil, columnRef{}, false
	}
	return &FunctionWrapper{
		Name:     name,
		Schema:   schema,
		Args:     args,
		IsNested: false,
		Cast:     cast,
	}, col, true
}

// descendToWrapperRoot drills through the expression ladder looking for a
// Func_applicationContext or Func_expr_common_subexprContext. It collapses
// trivial single-child rule chains and unwraps redundant parentheses. When a
// typecast wraps the path it captures the OUTERMOST cast type and continues
// descent. funcCtx is nil when the path produces anything other than a
// function call (raw column, literal, expression, ...).
func descendToWrapperRoot(start antlr.ParserRuleContext) (cast string, funcCtx antlr.ParserRuleContext) {
	cur := start
	for cur != nil {
		switch n := cur.(type) {
		case *gen.A_expr_typecastContext:
			if tns := n.AllTypename(); len(tns) > 0 {
				cast = strings.TrimSpace(tns[len(tns)-1].GetText())
			}
			if n.C_expr() != nil {
				cur = n.C_expr().(antlr.ParserRuleContext)
				continue
			}
			return cast, nil
		case *gen.C_expr_exprContext:
			// CAST(... AS type) appears as a Func_expr → Func_expr_common_subexpr
			// with CAST() set; we surface that as a Cast on the wrapper, not as a
			// function call. Detect it here, peel it once.
			if n.Func_expr() != nil {
				if peeled, ok := peelCastCommonSubexpr(n.Func_expr()); ok {
					if cast == "" {
						cast = peeled.castType
					}
					cur = peeled.inner
					continue
				}
				cur = n.Func_expr().(antlr.ParserRuleContext)
				continue
			}
			if n.A_expr() != nil && n.OPEN_PAREN() != nil {
				cur = n.A_expr().(antlr.ParserRuleContext)
				continue
			}
			return cast, nil
		case *gen.Func_exprContext:
			if peeled, ok := peelCastCommonSubexpr(n); ok {
				if cast == "" {
					cast = peeled.castType
				}
				cur = peeled.inner
				continue
			}
			if n.Func_application() != nil {
				cur = n.Func_application().(antlr.ParserRuleContext)
				continue
			}
			if n.Func_expr_common_subexpr() != nil {
				cur = n.Func_expr_common_subexpr().(antlr.ParserRuleContext)
				continue
			}
			return cast, nil
		case *gen.Func_applicationContext, *gen.Func_expr_common_subexprContext:
			return cast, cur
		}
		next, ok := singleRuleChild(cur)
		if !ok {
			return cast, nil
		}
		cur = next
	}
	return cast, nil
}

type peeledCast struct {
	castType string
	inner    antlr.ParserRuleContext
}

// peelCastCommonSubexpr handles the CAST(<expr> AS <type>) form, which the
// grammar routes through Func_expr_common_subexpr. Returns ok=true with the
// inner expression and target type when this node is a CAST, ok=false
// otherwise (or for non-Func_expr / non-common-subexpr inputs).
func peelCastCommonSubexpr(in interface{}) (peeledCast, bool) {
	var cs *gen.Func_expr_common_subexprContext
	switch n := in.(type) {
	case *gen.Func_exprContext:
		if n.Func_expr_common_subexpr() == nil {
			return peeledCast{}, false
		}
		var ok bool
		cs, ok = n.Func_expr_common_subexpr().(*gen.Func_expr_common_subexprContext)
		if !ok {
			return peeledCast{}, false
		}
	case gen.IFunc_exprContext:
		if n.Func_expr_common_subexpr() == nil {
			return peeledCast{}, false
		}
		var ok bool
		cs, ok = n.Func_expr_common_subexpr().(*gen.Func_expr_common_subexprContext)
		if !ok {
			return peeledCast{}, false
		}
	case *gen.Func_expr_common_subexprContext:
		cs = n
	default:
		return peeledCast{}, false
	}
	if cs.CAST() == nil || cs.A_expr(0) == nil || cs.Typename() == nil {
		return peeledCast{}, false
	}
	return peeledCast{
		castType: strings.TrimSpace(cs.Typename().GetText()),
		inner:    cs.A_expr(0).(antlr.ParserRuleContext),
	}, true
}

// singleRuleChild returns the only ParserRuleContext child if this node has
// exactly one such child AND no terminal-node children that carry semantic
// meaning. Parentheses are tolerated; everything else (operators, AS, FROM,
// commas, etc.) means we cannot collapse the node to a single inner expr.
func singleRuleChild(ctx antlr.ParserRuleContext) (antlr.ParserRuleContext, bool) {
	var only antlr.ParserRuleContext
	for i := 0; i < ctx.GetChildCount(); i++ {
		child := ctx.GetChild(i)
		if rc, ok := child.(antlr.ParserRuleContext); ok {
			if only != nil {
				return nil, false
			}
			only = rc
			continue
		}
		if t, ok := child.(antlr.TerminalNode); ok {
			sym := t.GetSymbol()
			if sym == nil {
				continue
			}
			switch sym.GetTokenType() {
			case antlr.TokenEOF, gen.PostgreSQLLexerOPEN_PAREN, gen.PostgreSQLLexerCLOSE_PAREN:
				continue
			default:
				return nil, false
			}
		}
	}
	if only == nil {
		return nil, false
	}
	return only, true
}

// decodeAllowlistedCall inspects a Func_application or Func_expr_common_subexpr
// and, if the call is an allowlisted shape, returns the canonical name, the
// schema (always "" — pg_catalog is canonicalised away), the literal-arg list
// and the column-bearing expression. ok=false for any non-allowlisted call.
func decodeAllowlistedCall(node antlr.ParserRuleContext) (name, schema string, args []FunctionArg, columnExpr antlr.ParserRuleContext, ok bool) {
	switch n := node.(type) {
	case *gen.Func_applicationContext:
		return decodeFuncApplication(n)
	case *gen.Func_expr_common_subexprContext:
		return decodeFuncCommonSubexpr(n)
	}
	return "", "", nil, nil, false
}

func decodeFuncApplication(n *gen.Func_applicationContext) (string, string, []FunctionArg, antlr.ParserRuleContext, bool) {
	if n.Func_name() == nil {
		return "", "", nil, nil, false
	}
	rawName := strings.TrimSpace(n.Func_name().GetText())
	name := rawName
	if dot := strings.Index(rawName, "."); dot >= 0 {
		schemaPart := strings.ToLower(strings.TrimSpace(rawName[:dot]))
		if schemaPart != "pg_catalog" {
			return "", "", nil, nil, false
		}
		name = rawName[dot+1:]
	}
	canon := strings.ToLower(strings.TrimSpace(name))
	if _, allowed := allowedWrapperFunctions[canon]; !allowed {
		return "", "", nil, nil, false
	}
	arglist := n.Func_arg_list()
	if arglist == nil {
		return "", "", nil, nil, false
	}
	argExprs := arglist.AllFunc_arg_expr()
	if len(argExprs) == 0 {
		return "", "", nil, nil, false
	}
	switch canon {
	case "length", "lower", "upper", "char_length", "octet_length":
		if len(argExprs) != 1 {
			return "", "", nil, nil, false
		}
		return canon, "", nil, argExprToA_expr(argExprs[0]), true
	case "date_trunc":
		if len(argExprs) != 2 {
			return "", "", nil, nil, false
		}
		unitExpr := argExprToA_expr(argExprs[0])
		colExpr := argExprToA_expr(argExprs[1])
		return canon, "", []FunctionArg{literalArgFromA_expr(unitExpr)}, colExpr, true
	case "coalesce":
		if len(argExprs) < 2 {
			return "", "", nil, nil, false
		}
		colExpr := argExprToA_expr(argExprs[0])
		defaults := make([]FunctionArg, 0, len(argExprs)-1)
		for _, ae := range argExprs[1:] {
			defaults = append(defaults, literalArgFromA_expr(argExprToA_expr(ae)))
		}
		return canon, "", defaults, colExpr, true
	case "extract":
		// EXTRACT lives on the common-subexpr rule; if it lands here the AST is
		// in a shape we don't claim to handle.
		return "", "", nil, nil, false
	}
	return "", "", nil, nil, false
}

func decodeFuncCommonSubexpr(n *gen.Func_expr_common_subexprContext) (string, string, []FunctionArg, antlr.ParserRuleContext, bool) {
	if n.EXTRACT() != nil {
		if n.Extract_list() == nil {
			return "", "", nil, nil, false
		}
		el, ok := n.Extract_list().(*gen.Extract_listContext)
		if !ok || el.A_expr() == nil {
			return "", "", nil, nil, false
		}
		return "extract", "", []FunctionArg{extractFieldLiteral(el.Extract_arg())}, el.A_expr().(antlr.ParserRuleContext), true
	}
	if n.COALESCE() != nil {
		if n.Expr_list() == nil {
			return "", "", nil, nil, false
		}
		all := n.Expr_list().AllA_expr()
		if len(all) < 2 {
			return "", "", nil, nil, false
		}
		defaults := make([]FunctionArg, 0, len(all)-1)
		for _, ae := range all[1:] {
			defaults = append(defaults, literalArgFromA_expr(ae.(antlr.ParserRuleContext)))
		}
		return "coalesce", "", defaults, all[0].(antlr.ParserRuleContext), true
	}
	return "", "", nil, nil, false
}

func argExprToA_expr(ae gen.IFunc_arg_exprContext) antlr.ParserRuleContext {
	if ae == nil || ae.A_expr() == nil {
		return nil
	}
	return ae.A_expr().(antlr.ParserRuleContext)
}

// extractFieldLiteral handles the EXTRACT(field FROM ...) special AST. Field is
// one of the year/month/etc. keywords, an Identifier, an Sconst, or a PARAM.
// PARAM (placeholder) yields a non-literal arg (Literal: nil, IsNull: false).
func extractFieldLiteral(ea gen.IExtract_argContext) FunctionArg {
	if ea == nil {
		return FunctionArg{}
	}
	switch {
	case ea.YEAR_P() != nil:
		s := "year"
		return FunctionArg{Literal: &s}
	case ea.MONTH_P() != nil:
		s := "month"
		return FunctionArg{Literal: &s}
	case ea.DAY_P() != nil:
		s := "day"
		return FunctionArg{Literal: &s}
	case ea.HOUR_P() != nil:
		s := "hour"
		return FunctionArg{Literal: &s}
	case ea.MINUTE_P() != nil:
		s := "minute"
		return FunctionArg{Literal: &s}
	case ea.SECOND_P() != nil:
		s := "second"
		return FunctionArg{Literal: &s}
	case ea.Identifier() != nil:
		s := strings.ToLower(strings.TrimSpace(ea.Identifier().GetText()))
		if s == "" {
			return FunctionArg{}
		}
		return FunctionArg{Literal: &s}
	case ea.Sconst() != nil:
		s := stripStringQuotes(ea.Sconst().GetText())
		return FunctionArg{Literal: &s}
	case ea.PARAM() != nil:
		return FunctionArg{}
	}
	return FunctionArg{}
}

// literalArgFromA_expr extracts a SQL textual literal from a wrapper-arg
// position. Returns Literal=nil, IsNull=false for non-literal expressions
// (column refs, sub-calls, placeholders, casts). Returns IsNull=true for the
// bare NULL keyword.
func literalArgFromA_expr(expr antlr.ParserRuleContext) FunctionArg {
	if expr == nil {
		return FunctionArg{}
	}
	cur := expr
	for {
		// Stop at C_expr_exprContext so we can inspect its labelled productions
		// (Aexprconst / Columnref / PARAM) before any further structural descent
		// would peel them off into shape we don't recognise here.
		if _, ok := cur.(*gen.C_expr_exprContext); ok {
			break
		}
		// A_expr_typecast with at least one cast is a non-literal expression.
		if tc, ok := cur.(*gen.A_expr_typecastContext); ok {
			if len(tc.AllTYPECAST()) > 0 {
				return FunctionArg{}
			}
		}
		next, ok := singleRuleChild(cur)
		if !ok {
			break
		}
		cur = next
	}
	if c, ok := cur.(*gen.C_expr_exprContext); ok {
		if c.Aexprconst() != nil {
			text := strings.TrimSpace(c.Aexprconst().GetText())
			if strings.EqualFold(text, "NULL") {
				return FunctionArg{IsNull: true}
			}
			return FunctionArg{Literal: &text}
		}
		if c.PARAM() != nil {
			return FunctionArg{}
		}
	}
	return FunctionArg{}
}

// stripStringQuotes removes surrounding single quotes from an SQL string
// literal text, leaving doubled-quote escapes alone (callers don't need
// PostgreSQL-grade unescaping for the wrapper-arg use case).
func stripStringQuotes(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}

// bareColumnFromExpr returns the columnRef for an expression that reduces
// strictly to a single ColumnrefContext, with no operators, casts, function
// calls, or literals on the path. ok=false otherwise.
func bareColumnFromExpr(expr antlr.ParserRuleContext) (columnRef, bool) {
	cur := expr
	for cur != nil {
		switch n := cur.(type) {
		case *gen.ColumnrefContext:
			return parseColRefFromContext(n), true
		case *gen.A_expr_typecastContext:
			if len(n.AllTYPECAST()) > 0 {
				return columnRef{}, false
			}
			if n.C_expr() != nil {
				cur = n.C_expr().(antlr.ParserRuleContext)
				continue
			}
			return columnRef{}, false
		case *gen.C_expr_exprContext:
			if n.Columnref() != nil {
				if cr, ok := n.Columnref().(*gen.ColumnrefContext); ok {
					return parseColRefFromContext(cr), true
				}
				return columnRef{}, false
			}
			if n.A_expr() != nil && n.OPEN_PAREN() != nil {
				cur = n.A_expr().(antlr.ParserRuleContext)
				continue
			}
			return columnRef{}, false
		}
		next, ok := singleRuleChild(cur)
		if !ok {
			return columnRef{}, false
		}
		cur = next
	}
	return columnRef{}, false
}
