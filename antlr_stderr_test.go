package postgresparser

import (
	"io"
	"os"
	"sync"
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/valkdb/postgresparser/gen"
)

var stderrCaptureMu sync.Mutex

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()

	stderrCaptureMu.Lock()
	defer stderrCaptureMu.Unlock()

	r, w, err := os.Pipe()
	require.NoError(t, err)

	oldStderr := os.Stderr
	os.Stderr = w
	defer func() {
		os.Stderr = oldStderr
	}()

	fn()

	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	return string(out)
}

func TestANTLRDefaultListenersWriteSyntaxErrorsToStderr(t *testing.T) {
	stderr := captureStderr(t, func() {
		input := antlr.NewInputStream("SELECT FROM")
		lexer := gen.NewPostgreSQLLexer(input)
		stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
		parser := gen.NewPostgreSQLParser(stream)
		parser.BuildParseTrees = true
		_ = parser.Root()
	})

	assert.Contains(t, stderr, "line 1:")
}

func TestReplaceErrorListenersCapturesSyntaxErrorsWithoutStderr(t *testing.T) {
	errListener := &parseErrorListener{}
	stderr := captureStderr(t, func() {
		input := antlr.NewInputStream("SELECT FROM")
		lexer := gen.NewPostgreSQLLexer(input)
		stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
		parser := gen.NewPostgreSQLParser(stream)
		parser.BuildParseTrees = true

		replaceErrorListeners(lexer, errListener)
		replaceErrorListeners(parser, errListener)

		_ = parser.Root()
	})

	require.NotEmpty(t, errListener.errs)
	assert.Empty(t, stderr)
	assert.Contains(t, errListener.errs[0].Message, "mismatched input")
}

func TestParseSQLReportsSyntaxErrorsWithoutStderr(t *testing.T) {
	var err error
	stderr := captureStderr(t, func() {
		_, err = ParseSQL("SELECT FROM")
	})

	require.Error(t, err)

	var parseErr *ParseErrors
	require.ErrorAs(t, err, &parseErr)
	require.NotEmpty(t, parseErr.Errors)
	assert.Empty(t, stderr)
}
