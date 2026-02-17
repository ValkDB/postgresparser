// errors.go defines structured error types and the ANTLR listener used during parsing.
package postgresparser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"
)

// Sentinel errors returned by the SQL parsing functions.
var (
	// ErrNoStatements is returned when the input SQL contains no parseable statements.
	ErrNoStatements = errors.New("no statements found")

	// ErrMultipleStatements is returned by ParseSQLStrict when input contains
	// more than one statement.
	ErrMultipleStatements = errors.New("multiple statements found")

	// ErrNilContext is returned when a required parser context is nil.
	ErrNilContext = errors.New("nil context")
)

// MultipleStatementsError indicates ParseSQLStrict received a multi-statement input.
type MultipleStatementsError struct {
	StatementCount int
}

// Error formats the strict-mode multi-statement validation failure.
func (e *MultipleStatementsError) Error() string {
	return fmt.Sprintf("%s: expected exactly 1 statement, got %d", ErrMultipleStatements, e.StatementCount)
}

// Unwrap returns the sentinel error for errors.Is compatibility.
func (e *MultipleStatementsError) Unwrap() error {
	return ErrMultipleStatements
}

// SyntaxError describes a single parser syntax error with line/column context.
type SyntaxError struct {
	Line    int
	Column  int
	Message string
	// TokenIndex is the offending token index when available; -1 when unknown.
	TokenIndex int
}

// ParseErrors aggregates syntax errors encountered while parsing a SQL string.
type ParseErrors struct {
	SQL    string
	Errors []SyntaxError
}

// Error formats the aggregated syntax errors into a single string.
func (p *ParseErrors) Error() string {
	if p == nil || len(p.Errors) == 0 {
		return "parse error"
	}
	if len(p.Errors) == 1 {
		err := p.Errors[0]
		return fmt.Sprintf("parse error: line %d:%d %s", err.Line, err.Column, err.Message)
	}
	parts := make([]string, len(p.Errors))
	for i, err := range p.Errors {
		parts[i] = fmt.Sprintf("line %d:%d %s", err.Line, err.Column, err.Message)
	}
	return fmt.Sprintf("parse error(s): %s", strings.Join(parts, "; "))
}

// parseErrorListener collects syntax errors emitted by the ANTLR parser.
type parseErrorListener struct {
	antlr.DefaultErrorListener
	errs []SyntaxError
}

// SyntaxError records each ANTLR syntax error with position data for later consumption.
func (l *parseErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{},
	line, column int, msg string, e antlr.RecognitionException) {
	tokenIndex := -1
	if tok, ok := offendingSymbol.(antlr.Token); ok && tok != nil {
		tokenIndex = tok.GetTokenIndex()
	}
	l.errs = append(l.errs, SyntaxError{
		Line:       line,
		Column:     column,
		Message:    msg,
		TokenIndex: tokenIndex,
	})
}
