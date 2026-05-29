// parser_ir_error_test.go verifies structured error reporting from ParseSQL.
package postgresparser

import (
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
