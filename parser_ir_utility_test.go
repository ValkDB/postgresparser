package postgresparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIR_SetClientMinMessages verifies SET with PL/pgSQL log-level tokens
// that the grammar cannot parse (WARNING, NOTICE, DEBUG, INFO, EXCEPTION, ERROR).
func TestIR_SetClientMinMessages(t *testing.T) {
	levels := []string{"warning", "notice", "debug", "info", "exception", "error"}
	for _, level := range levels {
		t.Run(level, func(t *testing.T) {
			sql := "SET client_min_messages = " + level
			result, err := ParseSQL(sql)
			require.NoError(t, err, "SET client_min_messages = %s should not error", level)
			require.NotNil(t, result)
			assert.Equal(t, QueryCommandUnknown, result.Command)
			assert.Equal(t, sql, result.RawSQL)
		})
	}
}

// TestIR_SetLogLevelStatements verifies SET statements with log-level RHS tokens
// are recovered when parser errors point at the RHS token.
func TestIR_SetLogLevelStatements(t *testing.T) {
	tests := []string{
		"SET log_min_messages = warning",
		"SET log_min_error_statement = warning",
		"SET foo = warning",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			result, err := ParseSQL(sql)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, QueryCommandUnknown, result.Command)
			assert.Equal(t, sql, result.RawSQL)
		})
	}
}

// TestIR_SetClientMinMessagesWhitespaceAndSyntaxVariants ensures targeted
// recovery works for common formatting and syntax variants.
func TestIR_SetClientMinMessagesWhitespaceAndSyntaxVariants(t *testing.T) {
	tests := []string{
		"SET\tclient_min_messages = warning",
		"SET\nclient_min_messages = warning",
		"SET client_min_messages=warning",
		"SET client_min_messages TO warning",
		"SET client_min_messages = WARNING",
		"SET client_min_messages = Warning",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			result, err := ParseSQL(sql)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, QueryCommandUnknown, result.Command)
			assert.Equal(t, sql, result.RawSQL)
		})
	}
}

// TestIR_SetWithScope ensures SET SESSION/LOCAL variants are handled.
func TestIR_SetWithScope(t *testing.T) {
	tests := []string{
		"SET SESSION client_min_messages = warning",
		"SET LOCAL client_min_messages = notice",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			result, err := ParseSQL(sql)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, QueryCommandUnknown, result.Command)
		})
	}
}

// TestIR_SetClientMinMessagesMalformed verifies malformed statements still
// return parse errors.
func TestIR_SetClientMinMessagesMalformed(t *testing.T) {
	tests := []string{
		"SET =",
		"SET client_min_messages =",
		"SET client_min_messages = warning]",
		"SET client_min_messages = warning;;",
		"SET client_min_messages = warning; SELECT 1",
		"SET log_min_messages = warning]",
		"SET log_min_messages = warning; SELECT 1",
		"SET debug = warning",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := ParseSQL(sql)
			assert.Error(t, err)
		})
	}
}

// TestIR_UtilityInvalidStatementsDoNotReturnUnknown hardens against silent false
// positives by asserting malformed utility SQL always returns a parse error.
func TestIR_UtilityInvalidStatementsDoNotReturnUnknown(t *testing.T) {
	tests := []string{
		"SET",
		"SET ;",
		"SET LOCAL",
		"SET SESSION",
		"SET client_min_messages",
		"SET client_min_messages TO",
		"SET client_min_messages =",
		"SET client_min_messages = warning]",
		"SET client_min_messages = warning extra",
		"SET client_min_messages TO warning extra",
		"SET log_min_messages = warning]",
		"SET log_min_messages = warning extra",
		"SET log_min_error_statement TO warning extra",
		"SET SESSION LOCAL foo = warning",
		"SET LOCAL SESSION foo = warning",
		"SET = warning",
		"SET debug = warning",
		"SET log_min_messages == warning",
		"SHOW",
		"SHOW ;",
		"SHOW ALL extra",
		"RESET",
		"RESET ;",
		"RESET ALL extra",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			result, err := ParseSQL(sql)
			require.Error(t, err)
			assert.Nil(t, result)
		})
	}
}

// TestIR_SetClientMinMessagesOtherValues confirms ordinary identifier values
// keep normal parser behavior and are not subject to targeted recovery.
func TestIR_SetClientMinMessagesOtherValues(t *testing.T) {
	tests := []string{
		"SET client_min_messages = foo",
		"SET client_min_messages = custom",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			result, err := ParseSQL(sql)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, QueryCommandUnknown, result.Command)
		})
	}
}

// TestIR_SetNumeric confirms SET with numeric values parses natively.
func TestIR_SetNumeric(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"statement_timeout", "SET statement_timeout = 0"},
		{"lock_timeout", "SET lock_timeout = 5000"},
		{"idle_in_transaction", "SET idle_in_transaction_session_timeout = 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseSQL(tt.sql)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, QueryCommandUnknown, result.Command)
			assert.Equal(t, tt.sql, result.RawSQL)
		})
	}
}

// TestIR_SetIdentifier confirms SET with identifier values parses natively.
func TestIR_SetIdentifier(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"search_path_single", "SET search_path = public"},
		{"search_path_multi", "SET search_path = public, pg_catalog"},
		{"check_function_bodies", "SET check_function_bodies = false"},
		{"xmloption", "SET xmloption = content"},
		{"row_security", "SET row_security = off"},
		{"default_table_access_method", "SET default_table_access_method = heap"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseSQL(tt.sql)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, QueryCommandUnknown, result.Command)
			assert.Equal(t, tt.sql, result.RawSQL)
		})
	}
}

// TestIR_SetString confirms SET with quoted string values parses natively.
func TestIR_SetString(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"work_mem", "SET work_mem = '64MB'"},
		{"datestyle", "SET DateStyle = 'ISO, MDY'"},
		{"timezone", "SET timezone = 'UTC'"},
		{"client_encoding", "SET client_encoding = 'UTF8'"},
		{"empty_string", "SET default_tablespace = ''"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseSQL(tt.sql)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, QueryCommandUnknown, result.Command)
			assert.Equal(t, tt.sql, result.RawSQL)
		})
	}
}

// TestIR_SetToSyntax confirms the SET ... TO syntax works alongside SET ... =.
func TestIR_SetToSyntax(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"identifier", "SET search_path TO public"},
		{"numeric", "SET statement_timeout TO 0"},
		{"string", "SET timezone TO 'America/New_York'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseSQL(tt.sql)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, QueryCommandUnknown, result.Command)
			assert.Equal(t, tt.sql, result.RawSQL)
		})
	}
}

// TestIR_ShowStatement verifies SHOW returns UNKNOWN without error.
func TestIR_ShowStatement(t *testing.T) {
	tests := []string{
		"SHOW search_path",
		"SHOW ALL",
		"SHOW server_version",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			result, err := ParseSQL(sql)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, QueryCommandUnknown, result.Command)
		})
	}
}

// TestIR_ResetStatement verifies RESET returns UNKNOWN without error.
func TestIR_ResetStatement(t *testing.T) {
	tests := []string{
		"RESET client_min_messages",
		"RESET ALL",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			result, err := ParseSQL(sql)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, QueryCommandUnknown, result.Command)
		})
	}
}

// TestIR_UtilityFalsePositiveGuard ensures words that merely start with
// SET/SHOW/RESET but are not utility statements still produce errors.
func TestIR_UtilityFalsePositiveGuard(t *testing.T) {
	// "SETTINGS" starts with "SET" but is not a valid utility statement.
	_, err := ParseSQL("SETTINGS foo = bar")
	assert.Error(t, err, "SETTINGS should not be treated as a utility statement")

	// Incomplete utility statements must still return parse errors.
	_, err = ParseSQL("SHOW")
	assert.Error(t, err, "invalid SHOW should still error")
	_, err = ParseSQL("RESET")
	assert.Error(t, err, "invalid RESET should still error")

	// Genuine syntax errors must still propagate.
	_, err = ParseSQL("SELECT FROM WHERE")
	assert.Error(t, err, "invalid SELECT should still error")
}
