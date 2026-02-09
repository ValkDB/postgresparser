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
