package postgresparser

import (
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/require"

	"github.com/valkdb/postgresparser/gen"
)

func TestLexer_JSONBCompoundQuestionTokens(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantText  string
		wantToken int
	}{
		{"question_or", "SELECT * FROM t WHERE data ?| array['a']", "?|", gen.PostgreSQLLexerQUESTION_OR},
		{"question_and", "SELECT * FROM t WHERE data ?& array['a']", "?&", gen.PostgreSQLLexerQUESTION_AND},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := antlr.NewInputStream(tc.sql)
			lexer := gen.NewPostgreSQLLexer(input)

			var found antlr.Token
			for {
				tok := lexer.NextToken()
				if tok.GetTokenType() == antlr.TokenEOF {
					break
				}
				if tok.GetText() == tc.wantText {
					found = tok
					break
				}
			}

			require.NotNil(t, found, "expected to find %q in token stream", tc.wantText)
			require.Equal(t, tc.wantToken, found.GetTokenType(),
				"%q should lex as the named compound token, not generic Operator", tc.wantText)
			require.NotEqual(t, gen.PostgreSQLLexerOperator, found.GetTokenType(),
				"%q must not fall through to the generic Operator rule", tc.wantText)
		})
	}
}

func TestLexer_BareQuestionStillPlaceholder(t *testing.T) {
	input := antlr.NewInputStream("SELECT * FROM t WHERE id = ?")
	lexer := gen.NewPostgreSQLLexer(input)

	var found antlr.Token
	for {
		tok := lexer.NextToken()
		if tok.GetTokenType() == antlr.TokenEOF {
			break
		}
		if tok.GetText() == "?" {
			found = tok
			break
		}
	}

	require.NotNil(t, found)
	require.Equal(t, gen.PostgreSQLLexerPARAM, found.GetTokenType(),
		"bare `?` must still lex as PARAM (placeholder), not as a compound token")
}

func TestParseSQL_JSONBCompoundOperatorsStillParse(t *testing.T) {
	queries := []string{
		"SELECT * FROM t WHERE data ?| array['a','b']",
		"SELECT * FROM t WHERE data ?& array['a','b']",
		"SELECT * FROM t WHERE meta ? 'key'",
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			result, err := ParseSQL(q)
			require.NoError(t, err)
			require.Empty(t, result.Placeholders,
				"JSONB `?`/`?|`/`?&` operators must not be reported as placeholders")
		})
	}
}
