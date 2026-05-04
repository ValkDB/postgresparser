package postgresparser

import (
	"sort"
	"strconv"
	"strings"

	"github.com/antlr4-go/antlr/v4"

	"github.com/valkdb/postgresparser/gen"
)

// placeholderContext records one parameter token plus its parser-rule ancestry.
type placeholderContext struct {
	token     antlr.Token
	ancestors []antlr.ParserRuleContext
}

// extractPlaceholders returns placeholder occurrences from a parsed top-level statement.
func extractPlaceholders(stmt gen.IStmtContext, sourceSQL string) []Placeholder {
	tree, ok := stmt.(antlr.ParseTree)
	if !ok {
		return nil
	}
	return extractPlaceholdersFromTree(tree, sourceSQL)
}

// extractPlaceholdersFromPreparable returns placeholder occurrences from a nested preparable statement.
func extractPlaceholdersFromPreparable(stmt gen.IPreparablestmtContext, sourceSQL string) []Placeholder {
	tree, ok := stmt.(antlr.ParseTree)
	if !ok {
		return nil
	}
	return extractPlaceholdersFromTree(tree, sourceSQL)
}

// extractPlaceholdersFromTree walks a parse subtree and classifies all real parameter tokens.
func extractPlaceholdersFromTree(tree antlr.ParseTree, sourceSQL string) []Placeholder {
	if tree == nil {
		return nil
	}

	var contexts []placeholderContext
	walkPlaceholderTree(tree, nil, &contexts)
	if len(contexts) == 0 {
		return nil
	}

	sort.SliceStable(contexts, func(i, j int) bool {
		return contexts[i].token.GetStart() < contexts[j].token.GetStart()
	})

	placeholders := make([]Placeholder, 0, len(contexts))
	nextQuestionIndex := 1
	for _, ctx := range contexts {
		if isOperatorPlaceholder(ctx.ancestors) {
			continue
		}

		text := ctx.token.GetText()
		ph := Placeholder{
			Style: tokenPlaceholderStyle(text),
			Start: sourceByteOffset(sourceSQL, ctx.token.GetStart()),
			End:   sourceByteOffset(sourceSQL, ctx.token.GetStop()+1),
		}
		if strings.HasPrefix(text, "$") {
			if idx, err := strconv.Atoi(strings.TrimPrefix(text, "$")); err == nil {
				ph.Index = idx
			}
		} else {
			ph.Index = nextQuestionIndex
			nextQuestionIndex++
		}

		classifyPlaceholder(&ph, ctx)
		placeholders = append(placeholders, ph)
	}
	return placeholders
}

// walkPlaceholderTree records PARAM terminals together with parser-rule ancestors.
func walkPlaceholderTree(tree antlr.ParseTree, ancestors []antlr.ParserRuleContext, out *[]placeholderContext) {
	if tree == nil {
		return
	}
	if term, ok := tree.(antlr.TerminalNode); ok {
		tok := term.GetSymbol()
		if tok != nil && tok.GetTokenType() == gen.PostgreSQLParserPARAM {
			copied := append([]antlr.ParserRuleContext(nil), ancestors...)
			*out = append(*out, placeholderContext{token: tok, ancestors: copied})
		}
		return
	}

	nextAncestors := ancestors
	if prc, ok := tree.(antlr.ParserRuleContext); ok {
		nextAncestors = append(nextAncestors, prc)
	}
	for i := 0; i < tree.GetChildCount(); i++ {
		child, ok := tree.GetChild(i).(antlr.ParseTree)
		if ok {
			walkPlaceholderTree(child, nextAncestors, out)
		}
	}
}

// classifyPlaceholder assigns the most specific syntactic role known for a placeholder.
func classifyPlaceholder(ph *Placeholder, ctx placeholderContext) {
	switch {
	case isIntervalPlaceholder(ctx):
		ph.Role = PlaceholderRoleIntervalOperand
	case hasAncestor[*gen.Limit_clauseContext](ctx.ancestors):
		ph.Role = PlaceholderRoleLimit
	case hasAncestor[*gen.Offset_clauseContext](ctx.ancestors):
		ph.Role = PlaceholderRoleOffset
	case hasAncestor[*gen.Set_clauseContext](ctx.ancestors):
		ph.Role = PlaceholderRoleUpdateSetValue
		ph.UpdateColumn = updateSetColumn(ctx)
	case hasAncestor[*gen.Values_clauseContext](ctx.ancestors):
		ph.Role = PlaceholderRoleInsertValue
		ph.InsertColumn = insertColumn(ctx)
	case nearestBetweenAncestor(ctx.ancestors) != nil:
		between := nearestBetweenAncestor(ctx.ancestors)
		ph.Role = betweenRole(ctx, between)
		ph.ColumnRef = comparisonColumnRef(ctx, between)
	case nearestInListAncestor(ctx.ancestors, ctx.token) != nil:
		inExpr := nearestInListAncestor(ctx.ancestors, ctx.token)
		ph.Role = PlaceholderRoleInListMember
		ph.ColumnRef = comparisonColumnRef(ctx, inExpr)
	case hasAncestor[*gen.Array_exprContext](ctx.ancestors):
		ph.Role = PlaceholderRoleArrayMember
		ph.ColumnRef = surroundingComparisonColumnRef(ctx)
	case nearestAncestor[*gen.Case_exprContext](ctx.ancestors) != nil:
		caseExpr := nearestAncestor[*gen.Case_exprContext](ctx.ancestors)
		ph.Role = PlaceholderRoleCaseExpr
		ph.CaseClause, ph.ColumnRef = casePlaceholderClause(ctx, caseExpr)
	case functionRef(ctx) != nil:
		fn := functionRef(ctx)
		ph.Role = PlaceholderRoleFunctionArg
		ph.ParentFn = fn
	case hasAncestor[*gen.Where_clauseContext](ctx.ancestors):
		ph.Role = PlaceholderRoleWhereValue
		ph.ColumnRef = surroundingComparisonColumnRef(ctx)
	case hasAncestor[*gen.Where_or_current_clauseContext](ctx.ancestors):
		ph.Role = PlaceholderRoleWhereValue
		ph.ColumnRef = surroundingComparisonColumnRef(ctx)
	case hasAncestor[*gen.Having_clauseContext](ctx.ancestors):
		ph.Role = PlaceholderRoleHavingValue
		ph.ColumnRef = surroundingComparisonColumnRef(ctx)
	case hasAncestor[*gen.Group_by_itemContext](ctx.ancestors):
		ph.Role = PlaceholderRoleGroupByOrdinal
	case hasAncestor[*gen.SortbyContext](ctx.ancestors):
		ph.Role = PlaceholderRoleOrderByOrdinal
	case hasRuleAncestor(ctx.ancestors, gen.PostgreSQLParserRULE_target_el):
		ph.Role = PlaceholderRoleSelectExpr
	default:
		ph.Role = PlaceholderRoleUnknown
	}
}

// tokenPlaceholderStyle maps raw parameter token text onto the public Style value.
func tokenPlaceholderStyle(text string) string {
	if strings.HasPrefix(text, "$") {
		return "$"
	}
	return "?"
}

// sourceByteOffset converts ANTLR rune offsets into Go string byte offsets.
func sourceByteOffset(source string, runeOffset int) int {
	if runeOffset <= 0 {
		return 0
	}
	seen := 0
	for i := range source {
		if seen == runeOffset {
			return i
		}
		seen++
	}
	if seen == runeOffset {
		return len(source)
	}
	return len(source)
}

// isOperatorPlaceholder identifies PARAM tokens used as PostgreSQL operators.
func isOperatorPlaceholder(ancestors []antlr.ParserRuleContext) bool {
	return hasAncestor[*gen.Qual_opContext](ancestors) ||
		hasAncestor[*gen.Qual_all_opContext](ancestors) ||
		hasAncestor[*gen.Subquery_OpContext](ancestors)
}

// nearestAncestor returns the nearest ancestor matching the requested parser context type.
func nearestAncestor[T antlr.ParserRuleContext](ancestors []antlr.ParserRuleContext) T {
	var zero T
	for i := len(ancestors) - 1; i >= 0; i-- {
		if ctx, ok := ancestors[i].(T); ok {
			return ctx
		}
	}
	return zero
}

// hasAncestor reports whether any ancestor matches the requested parser context type.
func hasAncestor[T antlr.ParserRuleContext](ancestors []antlr.ParserRuleContext) bool {
	for i := len(ancestors) - 1; i >= 0; i-- {
		if _, ok := ancestors[i].(T); ok {
			return true
		}
	}
	return false
}

// hasRuleAncestor reports whether any ancestor has the requested ANTLR rule index.
func hasRuleAncestor(ancestors []antlr.ParserRuleContext, ruleIndex int) bool {
	for i := len(ancestors) - 1; i >= 0; i-- {
		if ancestors[i].GetRuleIndex() == ruleIndex {
			return true
		}
	}
	return false
}

// isIntervalPlaceholder detects INTERVAL ? operands in a constant expression.
func isIntervalPlaceholder(ctx placeholderContext) bool {
	aexpr := nearestAncestor[*gen.AexprconstContext](ctx.ancestors)
	if aexpr != nil && strings.HasPrefix(strings.ToUpper(aexpr.GetText()), "INTERVAL") {
		return true
	}
	cExpr := nearestAncestor[*gen.C_expr_exprContext](ctx.ancestors)
	return cExpr != nil && strings.HasPrefix(strings.ToUpper(cExpr.GetText()), "INTERVAL")
}

// nearestBetweenAncestor returns the nearest ancestor that contains actual BETWEEN syntax.
func nearestBetweenAncestor(ancestors []antlr.ParserRuleContext) *gen.A_expr_betweenContext {
	for i := len(ancestors) - 1; i >= 0; i-- {
		ctx, ok := ancestors[i].(*gen.A_expr_betweenContext)
		if ok && ctx.BETWEEN() != nil {
			return ctx
		}
	}
	return nil
}

// nearestInListAncestor returns the nearest ancestor where the token is in an IN expression list.
func nearestInListAncestor(ancestors []antlr.ParserRuleContext, tok antlr.Token) *gen.A_expr_inContext {
	for i := len(ancestors) - 1; i >= 0; i-- {
		ctx, ok := ancestors[i].(*gen.A_expr_inContext)
		if !ok || ctx.IN_P() == nil {
			continue
		}
		inList, ok := ctx.In_expr().(*gen.In_expr_listContext)
		if ok && containsToken(inList.Expr_list(), tok) {
			return ctx
		}
	}
	return nil
}

// betweenRole maps the first and second BETWEEN operands onto low/high roles.
func betweenRole(ctx placeholderContext, between *gen.A_expr_betweenContext) PlaceholderRole {
	if countParamTerminalsBefore(between, ctx.token) == 0 {
		return PlaceholderRoleBetweenLow
	}
	return PlaceholderRoleBetweenHigh
}

// updateSetColumn extracts the target column from an UPDATE SET assignment.
func updateSetColumn(ctx placeholderContext) string {
	setCtx := nearestAncestor[*gen.Set_clauseContext](ctx.ancestors)
	if setCtx == nil || setCtx.Set_target() == nil {
		return ""
	}

	target := setCtx.Set_target()
	parts := []string{}
	if target.Colid() != nil {
		parts = append(parts, target.Colid().GetText())
	}
	if opt := target.Opt_indirection(); opt != nil {
		for _, el := range opt.AllIndirection_el() {
			if indCtx, ok := el.(*gen.Indirection_elContext); ok && indCtx.Attr_name() != nil {
				parts = append(parts, indCtx.Attr_name().GetText())
			}
		}
	}
	return columnRefFromParts(parts).Name
}

// insertColumn maps a VALUES placeholder to its INSERT column-list entry when present.
func insertColumn(ctx placeholderContext) string {
	values := nearestAncestor[*gen.Values_clauseContext](ctx.ancestors)
	insert := nearestAncestor[*gen.InsertstmtContext](ctx.ancestors)
	if values == nil || insert == nil || insert.Insert_rest() == nil || insert.Insert_rest().Insert_column_list() == nil {
		return ""
	}

	columns := insertColumnNames(insert.Insert_rest().Insert_column_list())
	idx := countParamTerminalsBefore(values, ctx.token)
	if idx < 0 || idx >= len(columns) {
		return ""
	}
	return columns[idx]
}

// insertColumnNames returns normalized column names from an INSERT column list.
func insertColumnNames(list gen.IInsert_column_listContext) []string {
	if list == nil {
		return nil
	}
	items := list.AllInsert_column_item()
	cols := make([]string, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		text := strings.TrimSpace(item.GetText())
		if text != "" {
			cols = append(cols, columnRefFromParts([]string{text}).Name)
		}
	}
	return cols
}

// comparisonColumnRef extracts the other side's single column reference for a predicate placeholder.
func comparisonColumnRef(ctx placeholderContext, comp antlr.ParserRuleContext) string {
	child := comparisonOtherSide(comp, ctx.token)
	if child == nil {
		return ""
	}
	return singleColumnRef(child)
}

// surroundingComparisonColumnRef finds a comparison ancestor and extracts its non-placeholder column.
func surroundingComparisonColumnRef(ctx placeholderContext) string {
	if ref := ancestorComparisonColumnRef(ctx); ref != "" {
		return ref
	}
	if fn := nearestAncestor[*gen.Func_applicationContext](ctx.ancestors); fn != nil {
		fnCtx := placeholderContext{token: ctx.token, ancestors: ancestorsBefore(ctx.ancestors, fn)}
		return ancestorComparisonColumnRef(fnCtx)
	}
	return ""
}

// ancestorComparisonColumnRef extracts a column from the nearest predicate ancestor.
func ancestorComparisonColumnRef(ctx placeholderContext) string {
	if comp := nearestCompareAncestor(ctx.ancestors); comp != nil {
		return comparisonColumnRef(ctx, comp)
	}
	if comp := nearestQualOpAncestor(ctx.ancestors); comp != nil {
		return comparisonColumnRef(ctx, comp)
	}
	if comp := nearestInListAncestor(ctx.ancestors, ctx.token); comp != nil {
		return comparisonColumnRef(ctx, comp)
	}
	if comp := nearestBetweenAncestor(ctx.ancestors); comp != nil {
		return comparisonColumnRef(ctx, comp)
	}
	return ""
}

// nearestCompareAncestor returns the nearest ancestor with actual comparison syntax.
func nearestCompareAncestor(ancestors []antlr.ParserRuleContext) *gen.A_expr_compareContext {
	for i := len(ancestors) - 1; i >= 0; i-- {
		ctx, ok := ancestors[i].(*gen.A_expr_compareContext)
		if !ok {
			continue
		}
		if len(ctx.AllA_expr_like()) > 1 || ctx.Subquery_Op() != nil {
			return ctx
		}
	}
	return nil
}

// ancestorsBefore returns ancestors up to and excluding stop.
func ancestorsBefore(ancestors []antlr.ParserRuleContext, stop antlr.ParserRuleContext) []antlr.ParserRuleContext {
	for i := len(ancestors) - 1; i >= 0; i-- {
		if ancestors[i] == stop {
			return ancestors[:i]
		}
	}
	return ancestors
}

// nearestQualOpAncestor returns the nearest ancestor that contains a real custom operator.
func nearestQualOpAncestor(ancestors []antlr.ParserRuleContext) *gen.A_expr_qual_opContext {
	for i := len(ancestors) - 1; i >= 0; i-- {
		ctx, ok := ancestors[i].(*gen.A_expr_qual_opContext)
		if ok && len(ctx.AllQual_op()) > 0 {
			return ctx
		}
	}
	return nil
}

// comparisonOtherSide returns the sibling expression opposite the placeholder in a binary predicate.
func comparisonOtherSide(comp antlr.ParserRuleContext, tok antlr.Token) antlr.ParseTree {
	exprs := expressionChildren(comp)
	for _, expr := range exprs {
		if !containsToken(expr, tok) {
			return expr
		}
	}
	return nil
}

// expressionChildren returns parser-rule children that represent SQL expressions.
func expressionChildren(ctx antlr.ParserRuleContext) []antlr.ParseTree {
	children := make([]antlr.ParseTree, 0, 2)
	for i := 0; i < ctx.GetChildCount(); i++ {
		child, ok := ctx.GetChild(i).(antlr.ParseTree)
		if !ok {
			continue
		}
		if _, isTerminal := child.(antlr.TerminalNode); isTerminal {
			continue
		}
		children = append(children, child)
	}
	return children
}

// singleColumnRef returns a column name only when the expression resolves to one clear column.
func singleColumnRef(tree antlr.ParseTree) string {
	if tree == nil {
		return ""
	}
	if hasComplexExpressionOperator(tree.GetText()) {
		return ""
	}

	collector := &columnRefCollector{BasePostgreSQLParserListener: &gen.BasePostgreSQLParserListener{}}
	antlr.ParseTreeWalkerDefault.Walk(collector, tree)
	if len(collector.refs) != 1 {
		return ""
	}

	ref := parseColRefFromContext(collector.refs[0])
	return columnRefString(ref)
}

// hasComplexExpressionOperator rejects expressions where column attribution would be a guess.
func hasComplexExpressionOperator(text string) bool {
	for _, op := range []string{"||", "+", "-", "*", "/"} {
		if strings.Contains(text, op) {
			return true
		}
	}
	return false
}

// columnRefString renders a parsed column reference in stable public form.
func columnRefString(ref columnRef) string {
	if ref.Name == "" {
		return ""
	}
	if ref.TableAlias != "" {
		return ref.TableAlias + "." + ref.Name
	}
	return ref.Name
}

// casePlaceholderClause distinguishes predicate, result, and default CASE positions.
func casePlaceholderClause(ctx placeholderContext, caseExpr *gen.Case_exprContext) (CaseClause, string) {
	whenList := caseExpr.When_clause_list()
	if whenList != nil {
		for _, when := range whenList.AllWhen_clause() {
			whenCtx, ok := when.(*gen.When_clauseContext)
			if !ok {
				continue
			}
			exprs := whenCtx.AllA_expr()
			if len(exprs) > 0 && containsToken(exprs[0], ctx.token) {
				return CaseClausePredicate, singleColumnRef(exprs[0])
			}
			if len(exprs) > 1 && containsToken(exprs[1], ctx.token) {
				return CaseClauseResult, ""
			}
		}
	}
	if def := caseExpr.Case_default(); def != nil && containsToken(def, ctx.token) {
		return CaseClauseDefault, ""
	}
	return CaseClauseUnknown, ""
}

// functionRef returns metadata for placeholders that are direct function arguments.
func functionRef(ctx placeholderContext) *FunctionRef {
	if fn := nearestAncestor[*gen.Func_applicationContext](ctx.ancestors); fn != nil {
		name := strings.ToLower(fn.Func_name().GetText())
		args := fn.Func_arg_list()
		if args == nil {
			return &FunctionRef{Name: name}
		}
		allArgs := args.AllFunc_arg_expr()
		return &FunctionRef{Name: name, ArgIndex: argIndex(allArgs, ctx.token), ArgCount: len(allArgs)}
	}
	if common := nearestAncestor[*gen.Func_expr_common_subexprContext](ctx.ancestors); common != nil && common.EXTRACT() != nil {
		return &FunctionRef{Name: "extract", ArgIndex: 0, ArgCount: 2}
	}
	return nil
}

// argIndex returns the zero-based function argument index containing a token.
func argIndex(args []gen.IFunc_arg_exprContext, tok antlr.Token) int {
	for i, arg := range args {
		if containsToken(arg, tok) {
			return i
		}
	}
	return -1
}

// containsToken reports whether a parse tree contains the specific token instance.
func containsToken(tree any, tok antlr.Token) bool {
	if tree == nil || tok == nil {
		return false
	}
	antlrTree, ok := tree.(antlr.Tree)
	if !ok {
		return false
	}
	if term, ok := antlrTree.(antlr.TerminalNode); ok {
		return term.GetSymbol() != nil && term.GetSymbol().GetTokenIndex() == tok.GetTokenIndex()
	}
	for i := 0; i < antlrTree.GetChildCount(); i++ {
		if containsToken(antlrTree.GetChild(i), tok) {
			return true
		}
	}
	return false
}

// countParamTerminalsBefore counts PARAM terminals before tok within a subtree.
func countParamTerminalsBefore(tree antlr.Tree, tok antlr.Token) int {
	count := 0
	countParamTerminalsBeforeWalk(tree, tok, &count)
	return count
}

// countParamTerminalsBeforeWalk traverses a subtree until tok is found.
func countParamTerminalsBeforeWalk(tree antlr.Tree, tok antlr.Token, count *int) bool {
	if tree == nil || tok == nil {
		return false
	}
	if term, ok := tree.(antlr.TerminalNode); ok {
		sym := term.GetSymbol()
		if sym == nil {
			return false
		}
		if sym.GetTokenIndex() == tok.GetTokenIndex() {
			return true
		}
		if sym.GetTokenType() == gen.PostgreSQLParserPARAM {
			(*count)++
		}
		return false
	}
	for i := 0; i < tree.GetChildCount(); i++ {
		if countParamTerminalsBeforeWalk(tree.GetChild(i), tok, count) {
			return true
		}
	}
	return false
}
