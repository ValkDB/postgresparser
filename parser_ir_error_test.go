// parser_ir_error_test.go verifies structured error reporting from ParseSQL.
package postgresparser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIR_ErrorHandling checks a single syntax error includes line/column info.
func TestIR_ErrorHandling(t *testing.T) {
	badSQL := `SELECT FROM WHERE broken = true`
	_, err := ParseSQL(badSQL)
	require.Error(t, err, "expected parse error")

	perr, ok := err.(*ParseErrors)
	require.True(t, ok, "expected ParseErrors type, got %T", err)
	require.NotEmpty(t, perr.Errors, "expected at least one syntax error entry")

	first := perr.Errors[0]
	assert.Greater(t, first.Line, 0, "expected valid line number")
	assert.GreaterOrEqual(t, first.Column, 0, "expected valid column number")
	assert.NotEmpty(t, first.Message, "expected error message")
}

// TestIR_ErrorHandlingMultiple verifies aggregated errors still expose line info.
func TestIR_ErrorHandlingMultiple(t *testing.T) {
	badSQL := `
SELECT *
FROM
UNION
SELECT`
	_, err := ParseSQL(badSQL)
	require.Error(t, err, "expected parse error")

	perr, ok := err.(*ParseErrors)
	require.True(t, ok, "expected ParseErrors type, got %T", err)
	require.NotEmpty(t, perr.Errors, "expected at least one syntax error entry")

	assert.Contains(t, perr.Error(), "line", "expected error message to include line information")
}

// TestParseErrors_Error_NilReceiver validates Error() on a nil ParseErrors receiver.
func TestParseErrors_Error_NilReceiver(t *testing.T) {
	var pe *ParseErrors
	assert.Equal(t, "parse error", pe.Error(), "expected 'parse error'")
}

// TestParseErrors_Error_Empty checks Error() with an empty error list.
func TestParseErrors_Error_Empty(t *testing.T) {
	pe := &ParseErrors{SQL: "test", Errors: nil}
	assert.Equal(t, "parse error", pe.Error(), "expected 'parse error'")
}

// TestParseErrors_Error_Single verifies Error() formatting with one syntax error.
func TestParseErrors_Error_Single(t *testing.T) {
	pe := &ParseErrors{
		SQL:    "test",
		Errors: []SyntaxError{{Line: 1, Column: 5, Message: "bad token"}},
	}
	s := pe.Error()
	assert.Contains(t, s, "line 1:5", "unexpected error string")
	assert.Contains(t, s, "bad token", "unexpected error string")
}

// TestParseErrors_Error_Multiple confirms Error() formatting with multiple syntax errors.
func TestParseErrors_Error_Multiple(t *testing.T) {
	pe := &ParseErrors{
		SQL: "test",
		Errors: []SyntaxError{
			{Line: 1, Column: 5, Message: "bad token"},
			{Line: 2, Column: 3, Message: "unexpected EOF"},
		},
	}
	s := pe.Error()
	assert.Contains(t, s, "line 1:5", "expected error location 1")
	assert.Contains(t, s, "line 2:3", "expected error location 2")
	assert.Contains(t, s, "\n\n", "expected blank line separating error blocks")
}

// TestParseErrors_Error_RendersSingleCaret checks the caret line carries exactly
// one caret positioned at the reported column.
func TestParseErrors_Error_RendersSingleCaret(t *testing.T) {
	pe := &ParseErrors{
		SQL:    "SELECT FROM",
		Errors: []SyntaxError{{Line: 1, Column: 7, Message: "bad"}},
	}
	caret := caretLineOf(t, pe.Error())
	assert.Equal(t, "^", strings.TrimLeft(caret, " "), "expected a single caret")
	assert.Equal(t, len("  1 | ")+7, strings.IndexByte(caret, '^'), "caret should sit at the reported column")
}

// TestParseErrors_Error_LineOutOfRange checks errors pointing past the last
// source line fall back to header + message with no caret.
func TestParseErrors_Error_LineOutOfRange(t *testing.T) {
	pe := &ParseErrors{
		SQL:    "SELECT 1",
		Errors: []SyntaxError{{Line: 5, Column: 0, Message: "unexpected EOF"}},
	}
	s := pe.Error()
	assert.Contains(t, s, "parse error at line 5:0", "expected the header for the reported position")
	assert.Contains(t, s, "unexpected EOF", "expected the underlying message")
	assert.NotContains(t, s, "^", "expected no caret when the source line is unavailable")
}

// TestParseErrors_Error_SelectsReportedSourceLine checks the rendered block
// quotes the line named by the error, not earlier lines.
func TestParseErrors_Error_SelectsReportedSourceLine(t *testing.T) {
	pe := &ParseErrors{
		SQL:    "SELECT a\nFROM t\nWHERE",
		Errors: []SyntaxError{{Line: 3, Column: 0, Message: "incomplete"}},
	}
	s := pe.Error()
	assert.Contains(t, s, "3 | WHERE", "expected the line-3 source in the gutter")
	assert.NotContains(t, s, "SELECT a", "expected unrelated source lines to be omitted")
}

// TestParseErrors_Error_PreservesTabIndent checks the caret indent keeps tabs so
// alignment holds under tab-indented input.
func TestParseErrors_Error_PreservesTabIndent(t *testing.T) {
	pe := &ParseErrors{
		SQL:    "SELECT\n\tFROM x",
		Errors: []SyntaxError{{Line: 2, Column: 1, Message: "bad"}},
	}
	caret := caretLineOf(t, pe.Error())
	assert.Contains(t, caret, "\t", "caret indent should preserve the source tab")
}

func caretLineOf(t *testing.T, rendered string) string {
	t.Helper()
	for _, line := range strings.Split(rendered, "\n") {
		if strings.Contains(line, "^") {
			return line
		}
	}
	t.Fatalf("no caret line in rendered error:\n%s", rendered)
	return ""
}
