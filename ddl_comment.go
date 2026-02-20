package postgresparser

import (
	"fmt"
	"strings"
	"unicode"

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

type createTableElementComments struct {
	element  string
	comments []string
}

type createTableElementSplitter struct {
	runes []rune

	elements        []createTableElementComments
	current         strings.Builder
	pendingComments []string
	depth           int
	inSingle        bool
	inDouble        bool
	inDollar        bool
	dollarTag       string
	inBlockComment  bool
	hasContent      bool
}

// extractCreateTableFieldCommentsByColumn maps CREATE TABLE column names to
// line comments (`-- ...`) that immediately precede each column definition.
func extractCreateTableFieldCommentsByColumn(createStmtSQL string) map[string][]string {
	body := extractCreateTableBody(createStmtSQL)
	if body == "" {
		return nil
	}

	elements := splitCreateTableElementsWithComments(body)
	if len(elements) == 0 {
		return nil
	}

	commentsByColumn := make(map[string][]string)
	for _, elem := range elements {
		if len(elem.comments) == 0 {
			continue
		}
		colName := extractCreateTableColumnNameFromElement(elem.element)
		if colName == "" {
			continue
		}
		normalizedCol := normalizeCreateTableColumnName(colName)
		if normalizedCol == "" {
			continue
		}
		lines := make([]string, 0, len(elem.comments))
		for _, line := range elem.comments {
			trimmed := strings.TrimSpace(line)
			if trimmed != "" {
				lines = append(lines, trimmed)
			}
		}
		if len(lines) > 0 {
			commentsByColumn[normalizedCol] = lines
		}
	}

	if len(commentsByColumn) == 0 {
		return nil
	}
	return commentsByColumn
}

// extractCreateTableBody returns the SQL text inside the first top-level
// parentheses pair of the CREATE TABLE statement.
func extractCreateTableBody(sql string) string {
	runes := []rune(sql)
	if len(runes) == 0 {
		return ""
	}

	openIdx := -1
	depth := 0
	inSingle := false
	inDouble := false
	inDollar := false
	dollarTag := ""
	inLineComment := false
	inBlockComment := false

	for i := 0; i < len(runes); i++ {
		r := runes[i]

		if inLineComment {
			if r == '\n' || r == '\r' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			if r == '*' && i+1 < len(runes) && runes[i+1] == '/' {
				inBlockComment = false
				i++
			}
			continue
		}
		if inDollar {
			if r == '$' && hasDollarTerminator(runes, i, dollarTag) {
				i += len([]rune(dollarTag)) + 1
				inDollar = false
				dollarTag = ""
			}
			continue
		}
		if inSingle {
			if r == '\'' {
				if i+1 < len(runes) && runes[i+1] == '\'' {
					i++
				} else {
					inSingle = false
				}
			}
			continue
		}
		if inDouble {
			if r == '"' {
				if i+1 < len(runes) && runes[i+1] == '"' {
					i++
				} else {
					inDouble = false
				}
			}
			continue
		}

		if r == '-' && i+1 < len(runes) && runes[i+1] == '-' {
			inLineComment = true
			i++
			continue
		}
		if r == '/' && i+1 < len(runes) && runes[i+1] == '*' {
			inBlockComment = true
			i++
			continue
		}
		if r == '\'' {
			inSingle = true
			continue
		}
		if r == '"' {
			inDouble = true
			continue
		}
		if r == '$' {
			if tag, ok := parseDollarTag(runes, i); ok {
				inDollar = true
				dollarTag = tag
				i += len([]rune(tag)) + 1
				continue
			}
		}

		switch r {
		case '(':
			if openIdx < 0 {
				openIdx = i
				depth = 1
				continue
			}
			if depth > 0 {
				depth++
			}
		case ')':
			if depth > 0 {
				depth--
				if depth == 0 && openIdx >= 0 {
					return string(runes[openIdx+1 : i])
				}
			}
		}
	}

	return ""
}

// splitCreateTableElementsWithComments splits CREATE TABLE body elements by
// top-level commas while keeping preceding line comments for each element.
func splitCreateTableElementsWithComments(body string) []createTableElementComments {
	splitter := createTableElementSplitter{
		runes: []rune(body),
	}
	if len(splitter.runes) == 0 {
		return nil
	}

	for i := 0; i < len(splitter.runes); i++ {
		r := splitter.runes[i]

		if splitter.inBlockComment {
			if r == '*' && i+1 < len(splitter.runes) && splitter.runes[i+1] == '/' {
				splitter.inBlockComment = false
				i++
			}
			continue
		}
		if splitter.inDollar {
			splitter.current.WriteRune(r)
			if r == '$' && hasDollarTerminator(splitter.runes, i, splitter.dollarTag) {
				tagLen := len([]rune(splitter.dollarTag))
				for j := 1; j < tagLen+2 && i+j < len(splitter.runes); j++ {
					splitter.current.WriteRune(splitter.runes[i+j])
				}
				i += tagLen + 1
				splitter.inDollar = false
				splitter.dollarTag = ""
			}
			continue
		}
		if splitter.inSingle {
			splitter.current.WriteRune(r)
			if r == '\'' {
				if i+1 < len(splitter.runes) && splitter.runes[i+1] == '\'' {
					i++
					splitter.current.WriteRune(splitter.runes[i])
				} else {
					splitter.inSingle = false
				}
			}
			continue
		}
		if splitter.inDouble {
			splitter.current.WriteRune(r)
			if r == '"' {
				if i+1 < len(splitter.runes) && splitter.runes[i+1] == '"' {
					i++
					splitter.current.WriteRune(splitter.runes[i])
				} else {
					splitter.inDouble = false
				}
			}
			continue
		}

		if r == '-' && i+1 < len(splitter.runes) && splitter.runes[i+1] == '-' {
			commentStart := i + 2
			commentEnd := commentStart
			for commentEnd < len(splitter.runes) && splitter.runes[commentEnd] != '\n' && splitter.runes[commentEnd] != '\r' {
				commentEnd++
			}
			if !splitter.hasContent {
				commentLine := strings.TrimSpace(string(splitter.runes[commentStart:commentEnd]))
				if commentLine != "" {
					splitter.pendingComments = append(splitter.pendingComments, commentLine)
				}
			}
			i = commentEnd - 1
			continue
		}
		if r == '/' && i+1 < len(splitter.runes) && splitter.runes[i+1] == '*' {
			splitter.inBlockComment = true
			i++
			continue
		}
		if r == '\'' {
			splitter.inSingle = true
			splitter.current.WriteRune(r)
			splitter.hasContent = true
			continue
		}
		if r == '"' {
			splitter.inDouble = true
			splitter.current.WriteRune(r)
			splitter.hasContent = true
			continue
		}
		if r == '$' {
			if tag, ok := parseDollarTag(splitter.runes, i); ok {
				splitter.inDollar = true
				splitter.dollarTag = tag
				terminatorLen := len([]rune(tag)) + 2
				for j := 0; j < terminatorLen && i+j < len(splitter.runes); j++ {
					splitter.current.WriteRune(splitter.runes[i+j])
				}
				i += terminatorLen - 1
				splitter.hasContent = true
				continue
			}
		}

		if r == ',' && splitter.depth == 0 {
			splitter.flushCurrent()
			continue
		}
		if r == '(' {
			splitter.depth++
		} else if r == ')' && splitter.depth > 0 {
			splitter.depth--
		}

		splitter.current.WriteRune(r)
		if !unicode.IsSpace(r) {
			splitter.hasContent = true
		}
	}

	splitter.flushCurrent()
	if len(splitter.elements) == 0 {
		return nil
	}
	return splitter.elements
}

func (s *createTableElementSplitter) flushCurrent() {
	elem := strings.TrimSpace(s.current.String())
	if elem == "" {
		s.current.Reset()
		s.pendingComments = nil
		s.hasContent = false
		return
	}
	item := createTableElementComments{
		element: elem,
	}
	if len(s.pendingComments) > 0 {
		item.comments = append([]string(nil), s.pendingComments...)
	}
	s.elements = append(s.elements, item)
	s.current.Reset()
	s.pendingComments = nil
	s.hasContent = false
}

func extractCreateTableColumnNameFromElement(element string) string {
	trimmed := strings.TrimSpace(element)
	if trimmed == "" {
		return ""
	}
	token := readLeadingIdentifierToken(trimmed)
	if token == "" {
		return ""
	}
	if isCreateTableConstraintToken(token) {
		return ""
	}
	return token
}

func readLeadingIdentifierToken(s string) string {
	runes := []rune(strings.TrimSpace(s))
	if len(runes) == 0 {
		return ""
	}

	if runes[0] == '"' {
		for i := 1; i < len(runes); i++ {
			if runes[i] != '"' {
				continue
			}
			if i+1 < len(runes) && runes[i+1] == '"' {
				i++
				continue
			}
			return string(runes[:i+1])
		}
		return ""
	}

	for i, r := range runes {
		if unicode.IsSpace(r) || r == ',' || r == '(' || r == ')' {
			if i == 0 {
				return ""
			}
			return string(runes[:i])
		}
	}
	return string(runes)
}

func isCreateTableConstraintToken(token string) bool {
	switch strings.ToUpper(trimIdentQuotes(strings.TrimSpace(token))) {
	case "CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "EXCLUDE", "LIKE":
		return true
	default:
		return false
	}
}
