package postgresparser

import "strings"

type utilityCommand uint8

const (
	utilityCommandNone utilityCommand = iota
	utilityCommandSet
	utilityCommandShow
	utilityCommandReset
)

var recoverableSetLevels = map[string]struct{}{
	"WARNING":   {},
	"NOTICE":    {},
	"DEBUG":     {},
	"INFO":      {},
	"EXCEPTION": {},
	"ERROR":     {},
}

// shouldRecoverUtilityParseError controls parse-error fallback for utility
// statements. It first uses a cheap prefix precheck (no allocations on
// non-utility SQL), then strict statement-shape and token-position validation
// to avoid false UNKNOWN results on malformed input.
func shouldRecoverUtilityParseError(sql string, errs []SyntaxError) bool {
	cmd := detectUtilityCommandPrefix(sql)
	if cmd == utilityCommandNone {
		return false
	}

	if !isSingleStatementUtilityCandidate(sql) {
		return false
	}

	switch cmd {
	case utilityCommandSet:
		return isRecoverableSetStatement(sql, errs)
	case utilityCommandShow, utilityCommandReset:
		// No known parse-error grammar gap to recover here today.
		return false
	default:
		return false
	}
}

// isRecoverableSetStatement returns true only when SQL matches the strict SET
// shape, the RHS value token is one of the known problematic log-level tokens,
// and parser errors point at that RHS token.
func isRecoverableSetStatement(sql string, errs []SyntaxError) bool {
	setting, rhsToken, rhsStart, trimmedSQL, ok := parseSetRecoveryShape(sql)
	if !ok {
		return false
	}
	if setting == "" {
		return false
	}

	if _, ok := recoverableSetLevels[rhsToken]; !ok {
		return false
	}

	rhsLine, rhsColumn := lineAndColumnAtByteOffset(trimmedSQL, rhsStart)
	return hasSyntaxErrorAtToken(errs, rhsLine, rhsColumn, len(rhsToken))
}

// parseSetRecoveryShape parses a strict SET form:
// SET [SESSION|LOCAL] <setting> (=|TO) <rhsToken> [;]
// Exactly one optional scope keyword is allowed (SESSION or LOCAL, not both).
// It returns the setting name, RHS token (uppercased), RHS byte offset, and
// the trimmed SQL used for offset calculations.
func parseSetRecoveryShape(sql string) (setting string, rhsToken string, rhsStart int, trimmed string, ok bool) {
	trimmed = strings.TrimSpace(sql)
	if trimmed == "" {
		return "", "", 0, "", false
	}

	i := 0
	nextWord := func() (string, int, int, bool) {
		for i < len(trimmed) && isASCIISpace(trimmed[i]) {
			i++
		}
		if i >= len(trimmed) {
			return "", 0, 0, false
		}
		start := i
		for i < len(trimmed) && isWordChar(trimmed[i]) {
			i++
		}
		if start == i {
			return "", 0, 0, false
		}
		return trimmed[start:i], start, i, true
	}

	word, _, _, ok := nextWord()
	if !ok || !equalFoldASCIIWord(word, "SET") {
		return "", "", 0, "", false
	}

	// Optional scope.
	if peek, _, _, ok := nextWord(); ok && (equalFoldASCIIWord(peek, "SESSION") || equalFoldASCIIWord(peek, "LOCAL")) {
		// scope consumed
	} else if ok {
		// Not a scope token. Rewind the cursor by this token length so the same
		// token can be parsed again as the setting name in the next read.
		i -= len(peek)
	}

	settingWord, _, _, ok := nextWord()
	if !ok || !isSettingName(settingWord) {
		return "", "", 0, "", false
	}

	for i < len(trimmed) && isASCIISpace(trimmed[i]) {
		i++
	}
	if i >= len(trimmed) {
		return "", "", 0, "", false
	}

	// Operator: '=' or TO
	if trimmed[i] == '=' {
		i++
	} else {
		op, _, _, ok := nextWord()
		if !ok || !equalFoldASCIIWord(op, "TO") {
			return "", "", 0, "", false
		}
	}

	for i < len(trimmed) && isASCIISpace(trimmed[i]) {
		i++
	}
	if i >= len(trimmed) {
		return "", "", 0, "", false
	}

	rhsStart = i
	for i < len(trimmed) && isWordChar(trimmed[i]) {
		i++
	}
	if rhsStart == i {
		return "", "", 0, "", false
	}

	rhs := trimmed[rhsStart:i]

	for i < len(trimmed) && isASCIISpace(trimmed[i]) {
		i++
	}
	if i < len(trimmed) && trimmed[i] == ';' {
		i++
		for i < len(trimmed) && isASCIISpace(trimmed[i]) {
			i++
		}
	}
	if i != len(trimmed) {
		return "", "", 0, "", false
	}

	return strings.ToUpper(settingWord), strings.ToUpper(rhs), rhsStart, trimmed, true
}

// isSingleStatementUtilityCandidate returns true only when the input appears to
// be exactly one statement (allowing one optional trailing semicolon).
func isSingleStatementUtilityCandidate(sql string) bool {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return false
	}

	// Allow one optional trailing ';' but reject chained statements.
	if strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimSpace(trimmed[:len(trimmed)-1])
	}
	return trimmed != "" && !strings.Contains(trimmed, ";")
}

// lineAndColumnAtByteOffset returns 1-based line and 0-based column for a byte
// offset within a UTF-8 string. Column counts runes to align with ANTLR output.
func lineAndColumnAtByteOffset(s string, byteOffset int) (line int, column int) {
	if byteOffset < 0 {
		return 1, 0
	}
	if byteOffset > len(s) {
		byteOffset = len(s)
	}

	line = 1
	column = 0
	for i, r := range s {
		if i >= byteOffset {
			break
		}
		if r == '\n' {
			line++
			column = 0
			continue
		}
		column++
	}
	return line, column
}

// hasSyntaxErrorAtToken checks whether at least one parse error points at the
// target token location (line + column range), allowing small offsets within
// the token span.
func hasSyntaxErrorAtToken(errs []SyntaxError, line, column, tokenLen int) bool {
	if len(errs) == 0 {
		return false
	}
	if tokenLen <= 0 {
		tokenLen = 1
	}

	maxColumn := column + tokenLen
	for _, err := range errs {
		if err.Line != line {
			continue
		}
		if err.Column >= column && err.Column <= maxColumn {
			return true
		}
	}
	return false
}

// detectUtilityCommandPrefix performs a cheap case-insensitive prefix scan for
// SET/SHOW/RESET while avoiding allocations on non-utility SQL.
func detectUtilityCommandPrefix(sql string) utilityCommand {
	i := 0
	for i < len(sql) && isASCIISpace(sql[i]) {
		i++
	}
	if i >= len(sql) {
		return utilityCommandNone
	}

	start := i
	for i < len(sql) && isASCIIAlpha(sql[i]) {
		i++
	}
	if start == i {
		return utilityCommandNone
	}

	switch i - start {
	case 3:
		if equalFoldASCIIWord(sql[start:i], "SET") {
			return utilityCommandSet
		}
	case 4:
		if equalFoldASCIIWord(sql[start:i], "SHOW") {
			return utilityCommandShow
		}
	case 5:
		if equalFoldASCIIWord(sql[start:i], "RESET") {
			return utilityCommandReset
		}
	}

	return utilityCommandNone
}

// equalFoldASCIIWord compares ASCII words case-insensitively without allocations.
func equalFoldASCIIWord(value, want string) bool {
	if len(value) != len(want) {
		return false
	}
	for i := 0; i < len(value); i++ {
		if toUpperASCII(value[i]) != want[i] {
			return false
		}
	}
	return true
}

// toUpperASCII uppercases a single ASCII letter byte.
func toUpperASCII(b byte) byte {
	if b >= 'a' && b <= 'z' {
		return b - ('a' - 'A')
	}
	return b
}

// isASCIIAlpha reports whether b is an ASCII alphabetic letter.
func isASCIIAlpha(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// isASCIISpace reports whether b is one of the ASCII whitespace characters.
func isASCIISpace(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	default:
		return false
	}
}

// isWordChar reports whether b is allowed in unquoted utility identifiers/values.
func isWordChar(b byte) bool {
	return isASCIIAlpha(b) || (b >= '0' && b <= '9') || b == '_' || b == '.'
}

// isSettingName validates an unquoted GUC-style setting token.
func isSettingName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isWordChar(s[i]) {
			return false
		}
	}
	return true
}
