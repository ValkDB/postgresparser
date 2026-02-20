package postgresparser

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"

	"github.com/valkdb/postgresparser/gen"
)

// populateCommentStmt handles COMMENT ON metadata extraction.
func populateCommentStmt(result *ParsedQuery, ctx gen.ICommentstmtContext, tokens antlr.TokenStream) error {
	if ctx == nil {
		return fmt.Errorf("comment statement: %w", ErrNilContext)
	}

	action := DDLAction{
		Type:    DDLComment,
		Comment: decodeCommentText(ctx.Comment_text(), tokens),
	}

	switch {
	case ctx.COLUMN() != nil && ctx.Any_name() != nil:
		action.ObjectType = "COLUMN"
		rawName := contextText(tokens, ctx.Any_name())
		action.Target = rawName
		schema, tableName, columnName := splitQualifiedColumnName(rawName)
		action.Schema = schema
		action.ObjectName = tableName
		if columnName != "" {
			action.Columns = []string{columnName}
		}
		if tableName != "" {
			tableRaw := tableName
			if schema != "" {
				tableRaw = schema + "." + tableName
			}
			result.Tables = append(result.Tables, TableRef{
				Schema: schema,
				Name:   tableName,
				Type:   TableTypeBase,
				Raw:    tableRaw,
			})
		}

	case ctx.Object_type_any_name() != nil && ctx.Any_name() != nil:
		action.ObjectType = strings.ToUpper(normalizeSpace(contextText(tokens, ctx.Object_type_any_name())))
		rawName := contextText(tokens, ctx.Any_name())
		action.Target = rawName
		schema, objectName := splitQualifiedName(rawName)
		action.Schema = schema
		action.ObjectName = objectName
		if action.ObjectType == "TABLE" || action.ObjectType == "FOREIGN TABLE" {
			result.Tables = append(result.Tables, TableRef{
				Schema: schema,
				Name:   objectName,
				Type:   TableTypeBase,
				Raw:    rawName,
			})
		}

	case ctx.Object_type_name() != nil && ctx.Name() != nil:
		action.ObjectType = strings.ToUpper(normalizeSpace(contextText(tokens, ctx.Object_type_name())))
		rawName := contextText(tokens, ctx.Name())
		action.Target = rawName
		schema, objectName := splitQualifiedName(rawName)
		action.Schema = schema
		action.ObjectName = objectName

	case ctx.TYPE_P() != nil && len(ctx.AllTypename()) > 0:
		action.ObjectType = "TYPE"
		rawName := contextText(tokens, ctx.AllTypename()[0])
		action.Target = rawName
		schema, objectName := splitQualifiedName(rawName)
		action.Schema = schema
		action.ObjectName = objectName

	case ctx.DOMAIN_P() != nil && len(ctx.AllTypename()) > 0:
		action.ObjectType = "DOMAIN"
		rawName := contextText(tokens, ctx.AllTypename()[0])
		action.Target = rawName
		schema, objectName := splitQualifiedName(rawName)
		action.Schema = schema
		action.ObjectName = objectName

	default:
		action.ObjectType = "UNKNOWN"
	}

	result.DDLActions = append(result.DDLActions, action)
	return nil
}

func decodeCommentText(commentCtx gen.IComment_textContext, tokens antlr.TokenStream) string {
	if commentCtx == nil {
		return ""
	}
	raw := contextText(tokens, commentCtx)
	if raw == "" || strings.EqualFold(raw, "NULL") {
		return ""
	}
	return decodeCommentStringLiteral(raw)
}

func decodeCommentStringLiteral(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "$") {
		if decoded, ok := decodeDollarQuotedString(trimmed); ok {
			return decoded
		}
	}

	upper := strings.ToUpper(trimmed)
	switch {
	case strings.HasPrefix(upper, "E'") || strings.HasPrefix(upper, "N'"):
		decoded, ok := decodeSingleQuoted(trimmed[1:])
		if !ok {
			return trimmed
		}
		return decodeEscapedString(decoded)
	case strings.HasPrefix(upper, "U&'"):
		decoded, ok := decodeSingleQuoted(trimmed[2:])
		if !ok {
			return trimmed
		}
		return decoded
	}

	if decoded, ok := decodeSingleQuoted(trimmed); ok {
		return decoded
	}
	return trimmed
}

func decodeSingleQuoted(raw string) (string, bool) {
	if len(raw) < 2 || raw[0] != '\'' || raw[len(raw)-1] != '\'' {
		return "", false
	}
	inner := raw[1 : len(raw)-1]
	return strings.ReplaceAll(inner, "''", "'"), true
}

// escapedStringReplacer handles common C-style escape sequences in PostgreSQL
// E'...' string literals. Allocated once to avoid per-call allocations.
var escapedStringReplacer = strings.NewReplacer(
	`\\`, `\`,
	`\'`, `'`,
	`\n`, "\n",
	`\r`, "\r",
	`\t`, "\t",
	`\b`, "\b",
	`\f`, "\f",
)

func decodeEscapedString(raw string) string {
	return escapedStringReplacer.Replace(raw)
}

func decodeDollarQuotedString(raw string) (string, bool) {
	if raw == "" || raw[0] != '$' {
		return "", false
	}
	secondDollar := strings.IndexByte(raw[1:], '$')
	if secondDollar < 0 {
		return "", false
	}
	delimEnd := secondDollar + 1
	delim := raw[:delimEnd+1]
	if len(raw) < len(delim)*2 || !strings.HasSuffix(raw, delim) {
		return "", false
	}
	return raw[len(delim) : len(raw)-len(delim)], true
}

func splitQualifiedColumnName(name string) (schema, table, column string) {
	parts := splitQuotedDot(strings.TrimSpace(name))
	if len(parts) == 0 {
		return "", "", ""
	}
	column = strings.TrimSpace(parts[len(parts)-1])
	if len(parts) == 1 {
		return "", "", column
	}
	table = strings.TrimSpace(parts[len(parts)-2])
	if len(parts) == 2 {
		return "", table, column
	}
	schema = strings.TrimSpace(strings.Join(parts[:len(parts)-2], "."))
	return schema, table, column
}

func contextText(tokens antlr.TokenStream, ctx antlr.RuleContext) string {
	return strings.TrimSpace(ctxText(tokens, ctx))
}

// extractColumnLeadingLineComments retrieves SQL line comments (-- ...)
// from the ANTLR hidden channel that immediately precede the given column
// definition. It uses CommonTokenStream.GetHiddenTokensToLeft to walk
// backwards from the column's start token to the nearest default-channel
// token, collecting only LineComment tokens.
func extractColumnLeadingLineComments(colDef gen.IColumnDefContext, stream antlr.TokenStream) []string {
	if colDef == nil {
		return nil
	}
	cts, ok := stream.(*antlr.CommonTokenStream)
	if !ok {
		return nil
	}

	prc, ok := colDef.(antlr.ParserRuleContext)
	if !ok {
		return nil
	}
	startToken := prc.GetStart()
	if startToken == nil {
		return nil
	}

	tokenIndex := startToken.GetTokenIndex()
	if tokenIndex <= 0 {
		return nil
	}

	// channel -1 means "any non-default channel token".
	hiddenTokens := cts.GetHiddenTokensToLeft(tokenIndex, -1)

	var comments []string
	for _, tok := range hiddenTokens {
		if tok.GetTokenType() == gen.PostgreSQLLexerLineComment {
			text := tok.GetText()
			// Strip the leading "--" and trim whitespace.
			text = strings.TrimPrefix(text, "--")
			text = strings.TrimSpace(text)
			if text != "" {
				comments = append(comments, text)
			}
		}
	}
	return comments
}
