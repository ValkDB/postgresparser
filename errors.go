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
	// TokenLength is the rune length of the offending token, used for caret
	// width. Zero when unknown, in which case a single caret is rendered.
	TokenLength int
}

// ParseErrors aggregates syntax errors encountered while parsing a SQL string.
type ParseErrors struct {
	SQL    string
	Errors []SyntaxError
}

// Error formats the aggregated syntax errors, rendering each as a source line
// with a caret pointing at the offending token. Multiple errors are separated
// by a blank line.
func (p *ParseErrors) Error() string {
	if p == nil || len(p.Errors) == 0 {
		return "parse error"
	}
	lines := strings.Split(p.SQL, "\n")
	blocks := make([]string, len(p.Errors))
	for i, err := range p.Errors {
		blocks[i] = formatSyntaxError(err, lines)
	}
	return strings.Join(blocks, "\n\n")
}

// formatSyntaxError renders one syntax error. When the source line is
// available it shows that line with a caret underneath; otherwise it falls
// back to the header plus message only.
func formatSyntaxError(err SyntaxError, lines []string) string {
	header := fmt.Sprintf("parse error at line %d:%d", err.Line, err.Column)

	idx := err.Line - 1
	if idx < 0 || idx >= len(lines) {
		return fmt.Sprintf("%s\n  %s", header, err.Message)
	}

	src := lines[idx]
	gutter := fmt.Sprintf("  %d | ", err.Line)
	caretPad := strings.Repeat(" ", len(gutter)) + caretIndent(src, err.Column)

	width := err.TokenLength
	if width < 1 {
		width = 1
	}
	caret := strings.Repeat("^", width)

	return fmt.Sprintf("%s\n%s%s\n%s%s\n%s%s",
		header, gutter, src, caretPad, caret, strings.Repeat(" ", len(gutter)), err.Message)
}

// caretIndent builds the whitespace that positions a caret under column col of
// src, preserving tabs so alignment holds in tab-indented input.
func caretIndent(src string, col int) string {
	if col < 0 {
		col = 0
	}
	runes := []rune(src)
	var b strings.Builder
	for i := 0; i < col; i++ {
		if i < len(runes) && runes[i] == '\t' {
			b.WriteByte('\t')
		} else {
			b.WriteByte(' ')
		}
	}
	return b.String()
}

// replaceErrorListeners removes ANTLR's default console listener so parse
// failures stay inside library results instead of going to process stderr.
func replaceErrorListeners(recognizer antlr.Recognizer, listeners ...antlr.ErrorListener) {
	if recognizer == nil {
		return
	}
	recognizer.RemoveErrorListeners()
	for _, listener := range listeners {
		if listener == nil {
			continue
		}
		recognizer.AddErrorListener(listener)
	}
}

// parseErrorListener collects syntax errors emitted by ANTLR recognizers.
type parseErrorListener struct {
	antlr.DefaultErrorListener
	errs []SyntaxError
}

// SyntaxError records each ANTLR syntax error with position data for later consumption.
func (l *parseErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{},
	line, column int, msg string, e antlr.RecognitionException) {
	tokenIndex := -1
	tokenLength := 0
	if tok, ok := offendingSymbol.(antlr.Token); ok && tok != nil {
		tokenIndex = tok.GetTokenIndex()
		tokenLength = len([]rune(tok.GetText()))
	}
	l.errs = append(l.errs, SyntaxError{
		Line:        line,
		Column:      column,
		Message:     msg,
		TokenIndex:  tokenIndex,
		TokenLength: tokenLength,
	})
}
