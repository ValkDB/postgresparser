// entry.go contains the ParseSQL entry point and statement dispatch logic.
package postgresparser

import (
	"strings"

	"github.com/antlr4-go/antlr/v4"

	"github.com/valkdb/postgresparser/gen"
)

// ParseSQL parses a PostgreSQL query using the ANTLR-generated parser and returns
// a structured representation with projections, relations, and auxiliary clauses.
func ParseSQL(sql string) (*ParsedQuery, error) {
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
		if shouldRecoverUtilityParseError(cleanSQL, errListener.errs) {
			return &ParsedQuery{
				Command: QueryCommandUnknown,
				RawSQL:  cleanSQL,
			}, nil
		}
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

	res := &ParsedQuery{
		Command:        QueryCommandUnknown,
		RawSQL:         strings.TrimSpace(cleanSQL),
		DerivedColumns: make(map[string]string),
	}

	mainStmt := stmts[0]
	switch {
	case mainStmt.Selectstmt() != nil:
		res.Command = QueryCommandSelect
		if err := populateSelect(res, mainStmt.Selectstmt(), stream); err != nil {
			return nil, err
		}
	case mainStmt.Insertstmt() != nil:
		res.Command = QueryCommandInsert
		if err := populateInsert(res, mainStmt.Insertstmt(), stream); err != nil {
			return nil, err
		}
	case mainStmt.Updatestmt() != nil:
		res.Command = QueryCommandUpdate
		if err := populateUpdate(res, mainStmt.Updatestmt(), stream); err != nil {
			return nil, err
		}
	case mainStmt.Deletestmt() != nil:
		res.Command = QueryCommandDelete
		if err := populateDelete(res, mainStmt.Deletestmt(), stream); err != nil {
			return nil, err
		}
	case mainStmt.Mergestmt() != nil:
		res.Command = QueryCommandMerge
		if err := populateMerge(res, mainStmt.Mergestmt(), stream); err != nil {
			return nil, err
		}
	case mainStmt.Createstmt() != nil:
		res.Command = QueryCommandDDL
		if err := populateCreateTable(res, mainStmt.Createstmt(), stream); err != nil {
			return nil, err
		}
	case mainStmt.Dropstmt() != nil:
		res.Command = QueryCommandDDL
		if err := populateDropStmt(res, mainStmt.Dropstmt(), stream); err != nil {
			return nil, err
		}
	case mainStmt.Altertablestmt() != nil:
		res.Command = QueryCommandDDL
		if err := populateAlterTable(res, mainStmt.Altertablestmt(), stream); err != nil {
			return nil, err
		}
	case mainStmt.Indexstmt() != nil:
		res.Command = QueryCommandDDL
		if err := populateCreateIndex(res, mainStmt.Indexstmt(), stream); err != nil {
			return nil, err
		}
	case mainStmt.Truncatestmt() != nil:
		res.Command = QueryCommandDDL
		if err := populateTruncate(res, mainStmt.Truncatestmt(), stream); err != nil {
			return nil, err
		}
	default:
		return res, nil
	}

	res.Parameters = extractParameters(cleanSQL)
	return res, nil
}
