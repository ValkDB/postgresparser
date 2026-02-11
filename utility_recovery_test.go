package postgresparser

import (
	"strings"
	"testing"
)

func TestShouldRecoverUtilityParseError(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		errs []SyntaxError
		want bool
	}{
		{
			name: "client_min_messages_rhs_error",
			sql:  "SET client_min_messages = warning",
			errs: rhsErrorFor("SET client_min_messages = warning"),
			want: true,
		},
		{
			name: "log_min_messages_rhs_error",
			sql:  "SET log_min_messages = warning",
			errs: rhsErrorFor("SET log_min_messages = warning"),
			want: true,
		},
		{
			name: "arbitrary_setting_rhs_error",
			sql:  "SET foo = warning",
			errs: rhsErrorFor("SET foo = warning"),
			want: true,
		},
		{
			name: "scope_to_form_rhs_error",
			sql:  "SET LOCAL foo TO notice",
			errs: rhsErrorFor("SET LOCAL foo TO notice"),
			want: true,
		},
		{
			name: "double_scope_not_allowed",
			sql:  "SET SESSION LOCAL foo = warning",
			errs: []SyntaxError{{Line: 1, Column: 12, Message: "dummy"}},
			want: false,
		},
		{
			name: "tab_newline_whitespace_rhs_error",
			sql:  "SET\tfoo =\nwarning",
			errs: rhsErrorFor("SET\tfoo =\nwarning"),
			want: true,
		},
		{
			name: "unsupported_level",
			sql:  "SET foo = custom",
			errs: rhsErrorFor("SET foo = custom"),
			want: false,
		},
		{
			name: "error_not_on_rhs_no_recovery",
			sql:  "SET foo = warning",
			errs: []SyntaxError{{Line: 1, Column: 4, Message: "dummy"}},
			want: false,
		},
		{
			name: "malformed_set",
			sql:  "SET =",
			errs: []SyntaxError{{Line: 1, Column: 4, Message: "dummy"}},
			want: false,
		},
		{
			name: "missing_rhs",
			sql:  "SET foo =",
			errs: []SyntaxError{{Line: 1, Column: 9, Message: "dummy"}},
			want: false,
		},
		{
			name: "invalid_suffix",
			sql:  "SET foo = warning extra",
			errs: []SyntaxError{{Line: 1, Column: 18, Message: "dummy"}},
			want: false,
		},
		{
			name: "broken_token",
			sql:  "SET foo = warning]",
			errs: []SyntaxError{{Line: 1, Column: 17, Message: "dummy"}},
			want: false,
		},
		{
			name: "multi_statement",
			sql:  "SET foo = warning; SELECT 1",
			errs: []SyntaxError{{Line: 1, Column: 20, Message: "dummy"}},
			want: false,
		},
		{
			name: "show_no_known_recovery_gap",
			sql:  "SHOW ALL",
			errs: []SyntaxError{{Line: 1, Column: 0, Message: "dummy"}},
			want: false,
		},
		{
			name: "reset_no_known_recovery_gap",
			sql:  "RESET ALL",
			errs: []SyntaxError{{Line: 1, Column: 0, Message: "dummy"}},
			want: false,
		},
		{
			name: "non_utility",
			sql:  "SELECT FROM WHERE",
			errs: []SyntaxError{{Line: 1, Column: 7, Message: "dummy"}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldRecoverUtilityParseError(tt.sql, tt.errs)
			if got != tt.want {
				t.Fatalf("shouldRecoverUtilityParseError(%q) = %v, want %v", tt.sql, got, tt.want)
			}
		})
	}
}

func BenchmarkShouldRecoverUtilityParseError_NonUtilityLargeInput(b *testing.B) {
	largeMalformed := "SELECT " + strings.Repeat("(", 1<<20)
	errs := []SyntaxError{{Line: 1, Column: 7, Message: "dummy"}}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = shouldRecoverUtilityParseError(largeMalformed, errs)
	}
}

func rhsErrorFor(sql string) []SyntaxError {
	_, rhsToken, rhsStart, trimmed, ok := parseSetRecoveryShape(sql)
	if !ok {
		return []SyntaxError{{Line: 1, Column: 0, Message: "dummy"}}
	}
	line, column := lineAndColumnAtByteOffset(trimmed, rhsStart)
	return []SyntaxError{{Line: line, Column: column, Message: "mismatched input '" + rhsToken + "'"}}
}
