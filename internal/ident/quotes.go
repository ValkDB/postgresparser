package ident

import "strings"

// TrimQuotes removes outer double-quote characters from identifier-like strings.
func TrimQuotes(s string) string {
	return strings.Trim(s, `"`)
}
