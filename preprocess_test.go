package postgresparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPreprocessRemovesDoubleSlashComments(t *testing.T) {
	sql := "SELECT * FROM users // custom meta"
	cleaned := preprocessSQLInput(sql)

	assert.Equal(t, "SELECT * FROM users", cleaned, "unexpected cleaned SQL")
}

func TestPreprocessPreservesUrlLiterals(t *testing.T) {
	sql := "SELECT 'http://example.com' AS url // comment"
	cleaned := preprocessSQLInput(sql)

	expected := "SELECT 'http://example.com' AS url"
	assert.Equal(t, expected, cleaned, "unexpected cleaned SQL")
}

func TestPreprocessPreservesDollarQuotedString(t *testing.T) {
	sql := "SELECT $$http://example.com//keep$$ AS url // comment"
	cleaned := preprocessSQLInput(sql)

	expected := "SELECT $$http://example.com//keep$$ AS url"
	assert.Equal(t, expected, cleaned, "unexpected cleaned SQL")
}

func TestPreprocessHandlesMultilineDollarQuotedString(t *testing.T) {
	sql := "SELECT $$line1\n//not comment\nline3$$ AS body // trailing"
	cleaned := preprocessSQLInput(sql)

	expected := "SELECT $$line1\n//not comment\nline3$$ AS body"
	assert.Equal(t, expected, cleaned, "unexpected cleaned SQL")
}

func TestParseDollarTagValid(t *testing.T) {
	runes := []rune("$foo$rest")
	tag, ok := parseDollarTag(runes, 0)
	require.True(t, ok, "expected parse success")
	assert.Equal(t, "foo", tag, "expected tag 'foo'")
}

func TestParseDollarTagInvalidChar(t *testing.T) {
	runes := []rune("$foo-bar$")
	tag, ok := parseDollarTag(runes, 0)
	assert.False(t, ok, "expected parse failure")
	assert.Empty(t, tag, "expected empty tag")
}

func TestHasDollarTerminator(t *testing.T) {
	runes := []rune("text$foo$rest")
	assert.True(t, hasDollarTerminator(runes, 4, "foo"), "expected terminator to be detected")
	assert.False(t, hasDollarTerminator(runes, 4, "bar"), "expected terminator mismatch")
}

func TestPreprocessTrimsWhitespace(t *testing.T) {
	sql := "  SELECT id FROM users   \n"
	cleaned := preprocessSQLInput(sql)

	assert.Equal(t, "SELECT id FROM users", cleaned, "expected trimmed SQL")
}
