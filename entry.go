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
// Use ParseSQLAll to parse all statements, or ParseSQLStrict to enforce exactly one.
func ParseSQL(sql string) (*ParsedQuery, error) {
	state, err := prepareParseState(sql)
	if err != nil {
		return nil, err
	}
	return parseStatementToIR(state.stmts[0], state.stream, state.cleanSQL)
}

// ParseSQLAll parses all SQL statements in the input string and returns a
// batch result containing each statement IR plus statement-level counters.
func ParseSQLAll(sql string) (*ParseBatchResult, error) {
	state, err := prepareParseState(sql)
	if err != nil {
		return nil, err
	}

	queries := make([]*ParsedQuery, 0, len(state.stmts))
	for _, stmt := range state.stmts {
		stmtSQL := statementText(state.stream, stmt)
		if stmtSQL == "" {
			stmtSQL = state.cleanSQL
		}
		query, parseErr := parseStatementToIR(stmt, state.stream, stmtSQL)
		if parseErr != nil {
			return nil, parseErr
		}
		queries = append(queries, query)
	}

	var warnings []ParseWarning
	if len(state.stmts) > 1 {
		warnings = append(warnings, ParseWarning{
			Code: ParseWarningCodeFirstStatementOnly,
			Message: fmt.Sprintf(
				"ParseSQL parses only the first statement; %d additional statement(s) detected",
				len(state.stmts)-1,
			),
		})
	}

	return &ParseBatchResult{
		Queries:          queries,
		Warnings:         warnings,
		TotalStatements:  len(state.stmts),
		ParsedStatements: len(queries),
	}, nil
}

// ParseSQLStrict parses input only when it contains exactly one SQL statement.
// It returns ErrMultipleStatements when more than one statement is present.
func ParseSQLStrict(sql string) (*ParsedQuery, error) {
	state, err := prepareParseState(sql)
	if err != nil {
		return nil, err
	}
	if len(state.stmts) != 1 {
		return nil, &MultipleStatementsError{StatementCount: len(state.stmts)}
	}
	return parseStatementToIR(state.stmts[0], state.stream, state.cleanSQL)
}

type parseState struct {
	cleanSQL string
	stream   antlr.TokenStream
	stmts    []gen.IStmtContext
}

// prepareParseState preprocesses SQL, runs the ANTLR parser once, and returns
// the parsed statement list plus shared token stream used for IR extraction.
func prepareParseState(sql string) (*parseState, error) {
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
	if len(errListener.errs) > 0 {
		return nil, &ParseErrors{SQL: cleanSQL, Errors: errListener.errs}
	}
	if root == nil || root.Stmtblock() == nil {
		return nil, ErrNoStatements
	}

	stmtMulti := root.Stmtblock().Stmtmulti()
	if stmtMulti == nil {
		return nil, ErrNoStatements
	}
	stmts := stmtMulti.AllStmt()
	if len(stmts) == 0 {
		return nil, ErrNoStatements
	}

	return &parseState{
		cleanSQL: cleanSQL,
		stream:   stream,
		stmts:    stmts,
	}, nil
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
