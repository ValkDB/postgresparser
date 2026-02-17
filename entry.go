// entry.go contains the ParseSQL entry point and statement dispatch logic.
package postgresparser

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"

	"github.com/valkdb/postgresparser/gen"
)

// ParseSQL parses only the first SQL statement in the input string.
// Additional statements are ignored for backward compatibility.
// RawSQL in the returned ParsedQuery preserves the full preprocessed input.
// Use ParseSQLAll to parse all statements, or ParseSQLStrict to enforce exactly one.
func ParseSQL(sql string) (*ParsedQuery, error) {
	state, err := prepareParseState(sql, false)
	if err != nil {
		return nil, err
	}
	return parseStatementToIR(state.stmts[0], state.stream, state.cleanSQL)
}

// ParseSQLAll parses all SQL statements in the input string and returns a
// batch result containing one per-statement parse result in source order.
// Individual statement failures do not abort the batch; check
// StatementParseResult.Query (nil on failure) and Warnings for details.
func ParseSQLAll(sql string) (*ParseBatchResult, error) {
	state, err := prepareParseState(sql, true)
	if err != nil {
		return nil, err
	}

	statements := make([]StatementParseResult, len(state.stmts))
	parsedCount := 0
	for i, stmt := range state.stmts {
		stmtSQL := statementText(state.stream, stmt)
		statements[i] = StatementParseResult{
			Index:  i + 1,
			RawSQL: stmtSQL,
		}
		if stmtSQL == "" {
			continue
		}
		query, parseErr := parseStatementToIR(stmt, state.stream, stmtSQL)
		if parseErr != nil {
			continue
		}
		statements[i].Query = query
		parsedCount++
	}

	for _, syntaxErr := range state.syntaxErrors {
		idx := statementIndexForSyntaxError(state.stmts, syntaxErr)
		if idx < 0 || idx >= len(statements) {
			continue
		}
		statements[idx].Warnings = append(statements[idx].Warnings, ParseWarning{
			Code: ParseWarningCodeSyntaxError,
			Message: fmt.Sprintf(
				"line %d:%d %s",
				syntaxErr.Line,
				syntaxErr.Column,
				syntaxErr.Message,
			),
		})
	}

	hasFailures := parsedCount != len(statements)
	if !hasFailures {
		for i := range statements {
			if len(statements[i].Warnings) > 0 {
				hasFailures = true
				break
			}
		}
	}

	return &ParseBatchResult{
		Statements:       statements,
		TotalStatements:  len(statements),
		ParsedStatements: parsedCount,
		HasFailures:      hasFailures,
	}, nil
}

// ParseSQLStrict parses input only when it contains exactly one SQL statement.
// It returns ErrMultipleStatements when more than one statement is present.
func ParseSQLStrict(sql string) (*ParsedQuery, error) {
	state, err := prepareParseState(sql, false)
	if err != nil {
		return nil, err
	}
	if len(state.stmts) > 1 {
		return nil, &MultipleStatementsError{StatementCount: len(state.stmts)}
	}
	return parseStatementToIR(state.stmts[0], state.stream, state.cleanSQL)
}

// parseState holds the shared ANTLR parse artifacts produced by prepareParseState
// for reuse across the single- and multi-statement entry points.
type parseState struct {
	cleanSQL     string
	stream       antlr.TokenStream
	stmts        []gen.IStmtContext
	syntaxErrors []SyntaxError
}

// prepareParseState preprocesses SQL, runs ANTLR parsing once, and returns the
// parsed statement list plus shared token stream used for IR extraction.
// When tolerateSyntaxErrors is false, any syntax error fails immediately.
// When true, syntax errors are collected into state.syntaxErrors as long as
// statement contexts can still be recovered.
func prepareParseState(sql string, tolerateSyntaxErrors bool) (*parseState, error) {
	cleanSQL := preprocessSQLInput(sql)
	input := antlr.NewInputStream(cleanSQL)
	lexer := gen.NewPostgreSQLLexer(input)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	parser := gen.NewPostgreSQLParser(stream)
	parser.BuildParseTrees = true

	errListener := &parseErrorListener{}
	parser.RemoveErrorListeners()
	parser.AddErrorListener(errListener)

	root := parser.Root()
	if !tolerateSyntaxErrors && len(errListener.errs) > 0 {
		return nil, &ParseErrors{SQL: cleanSQL, Errors: errListener.errs}
	}
	if root == nil || root.Stmtblock() == nil {
		if len(errListener.errs) > 0 {
			return nil, &ParseErrors{SQL: cleanSQL, Errors: errListener.errs}
		}
		return nil, ErrNoStatements
	}
	stmtMulti := root.Stmtblock().Stmtmulti()
	if stmtMulti == nil {
		if len(errListener.errs) > 0 {
			return nil, &ParseErrors{SQL: cleanSQL, Errors: errListener.errs}
		}
		return nil, ErrNoStatements
	}
	stmts := stmtMulti.AllStmt()
	if len(stmts) == 0 {
		if len(errListener.errs) > 0 {
			return nil, &ParseErrors{SQL: cleanSQL, Errors: errListener.errs}
		}
		return nil, ErrNoStatements
	}

	state := &parseState{
		cleanSQL: cleanSQL,
		stream:   stream,
		stmts:    stmts,
	}
	if tolerateSyntaxErrors {
		state.syntaxErrors = errListener.errs
	}
	return state, nil
}

// parseStatementToIR maps a single parsed statement node to ParsedQuery IR.
func parseStatementToIR(stmt gen.IStmtContext, stream antlr.TokenStream, rawSQL string) (*ParsedQuery, error) {
	res := &ParsedQuery{
		Command:        QueryCommandUnknown,
		RawSQL:         strings.TrimSpace(rawSQL),
		DerivedColumns: make(map[string]string),
	}

	switch {
	case stmt.Selectstmt() != nil:
		res.Command = QueryCommandSelect
		if err := populateSelect(res, stmt.Selectstmt(), stream); err != nil {
			return nil, err
		}
	case stmt.Insertstmt() != nil:
		res.Command = QueryCommandInsert
		if err := populateInsert(res, stmt.Insertstmt(), stream); err != nil {
			return nil, err
		}
	case stmt.Updatestmt() != nil:
		res.Command = QueryCommandUpdate
		if err := populateUpdate(res, stmt.Updatestmt(), stream); err != nil {
			return nil, err
		}
	case stmt.Deletestmt() != nil:
		res.Command = QueryCommandDelete
		if err := populateDelete(res, stmt.Deletestmt(), stream); err != nil {
			return nil, err
		}
	case stmt.Mergestmt() != nil:
		res.Command = QueryCommandMerge
		if err := populateMerge(res, stmt.Mergestmt(), stream); err != nil {
			return nil, err
		}
	case stmt.Createstmt() != nil:
		res.Command = QueryCommandDDL
		if err := populateCreateTable(res, stmt.Createstmt(), stream); err != nil {
			return nil, err
		}
	case stmt.Dropstmt() != nil:
		res.Command = QueryCommandDDL
		if err := populateDropStmt(res, stmt.Dropstmt(), stream); err != nil {
			return nil, err
		}
	case stmt.Altertablestmt() != nil:
		res.Command = QueryCommandDDL
		if err := populateAlterTable(res, stmt.Altertablestmt(), stream); err != nil {
			return nil, err
		}
	case stmt.Indexstmt() != nil:
		res.Command = QueryCommandDDL
		if err := populateCreateIndex(res, stmt.Indexstmt(), stream); err != nil {
			return nil, err
		}
	case stmt.Truncatestmt() != nil:
		res.Command = QueryCommandDDL
		if err := populateTruncate(res, stmt.Truncatestmt(), stream); err != nil {
			return nil, err
		}
	default:
		return res, nil
	}

	res.Parameters = extractParameters(rawSQL)
	return res, nil
}

// statementText extracts the exact SQL text for one statement node.
func statementText(stream antlr.TokenStream, stmt gen.IStmtContext) string {
	ruleCtx, ok := stmt.(antlr.RuleContext)
	if !ok {
		return ""
	}
	return strings.TrimSpace(ctxText(stream, ruleCtx))
}

// statementIndexForSyntaxError maps an ANTLR syntax error to the nearest parsed
// statement index using token bounds when available, then line bounds.
func statementIndexForSyntaxError(stmts []gen.IStmtContext, syntaxErr SyntaxError) int {
	if len(stmts) == 0 {
		return -1
	}

	if syntaxErr.TokenIndex >= 0 {
		for i, stmt := range stmts {
			startToken, stopToken := statementTokenBounds(stmt)
			if startToken < 0 || stopToken < startToken {
				continue
			}
			if syntaxErr.TokenIndex >= startToken && syntaxErr.TokenIndex <= stopToken {
				return i
			}
		}
	}

	line := syntaxErr.Line
	for i, stmt := range stmts {
		startLine, stopLine := statementLineBounds(stmt)
		if startLine == 0 && stopLine == 0 {
			continue
		}
		if line >= startLine && line <= stopLine {
			return i
		}
	}

	firstStart, _ := statementLineBounds(stmts[0])
	if firstStart > 0 && line < firstStart {
		return 0
	}
	return -1
}

// statementTokenBounds returns the start and stop token indices of a statement
// context, or (-1, -1) if unavailable.
func statementTokenBounds(stmt gen.IStmtContext) (int, int) {
	if stmt == nil {
		return -1, -1
	}
	start := stmt.GetStart()
	stop := stmt.GetStop()
	if start == nil || stop == nil {
		return -1, -1
	}
	return start.GetTokenIndex(), stop.GetTokenIndex()
}

// statementLineBounds returns the start and stop line numbers of a statement
// context, or (0, 0) if unavailable.
func statementLineBounds(stmt gen.IStmtContext) (int, int) {
	if stmt == nil {
		return 0, 0
	}
	start := stmt.GetStart()
	stop := stmt.GetStop()
	if start == nil && stop == nil {
		return 0, 0
	}

	startLine := 0
	stopLine := 0
	if start != nil {
		startLine = start.GetLine()
	}
	if stop != nil {
		stopLine = stop.GetLine()
	}
	if startLine > 0 && stopLine < startLine {
		stopLine = startLine
	}
	return startLine, stopLine
}
