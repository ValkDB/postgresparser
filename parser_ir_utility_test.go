package postgresparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertUnknownUtility(t *testing.T, sql string) {
	t.Helper()

	result, err := ParseSQL(sql)
	require.NoError(t, err, "expected utility statement to parse")
	require.NotNil(t, result)
	assert.Equal(t, QueryCommandUnknown, result.Command)
}

// TestIR_UtilitySetClientMinMessagesLogLevels verifies the issue #18 path and
// common syntax variants for client_min_messages log levels.
func TestIR_UtilitySetClientMinMessagesLogLevels(t *testing.T) {
	tests := []string{
		"SET client_min_messages = warning;",
		"SET client_min_messages = notice;",
		"SET client_min_messages = debug;",
		"SET client_min_messages = info;",
		"SET client_min_messages = exception;",
		"SET client_min_messages = error;",
		"SET SESSION client_min_messages TO notice;",
		"SET LOCAL client_min_messages = DEBUG;",
		"SET client_min_messages=warning;",
		"SET\nclient_min_messages\t=\twarning;",
		"SET client_min_messages = Warning;",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			assertUnknownUtility(t, sql)
		})
	}
}

// TestIR_UtilitySetValueForms verifies additional valid SET value forms.
func TestIR_UtilitySetValueForms(t *testing.T) {
	tests := []string{
		"SET statement_timeout = 0;",
		"SET lock_timeout = 5000;",
		"SET idle_in_transaction_session_timeout = 0;",
		"SET search_path = public;",
		"SET search_path = public, pg_catalog;",
		"SET check_function_bodies = false;",
		"SET row_security = off;",
		"SET default_table_access_method = heap;",
		"SET work_mem = '64MB';",
		"SET DateStyle = 'ISO, MDY';",
		"SET timezone = 'UTC';",
		"SET client_encoding = 'UTF8';",
		"SET default_tablespace = '';",
		"SET search_path TO public;",
		"SET statement_timeout TO 0;",
		"SET timezone TO 'America/New_York';",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			assertUnknownUtility(t, sql)
		})
	}
}

// TestIR_UtilityShowReset verifies valid SHOW/RESET utility statements.
func TestIR_UtilityShowReset(t *testing.T) {
	tests := []string{
		"SHOW search_path;",
		"SHOW ALL;",
		"SHOW server_version;",
		"RESET client_min_messages;",
		"RESET ALL;",
		"RESET TIME ZONE;",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			assertUnknownUtility(t, sql)
		})
	}
}

// TestIR_UtilityInvalidStatementsReturnError ensures malformed utility SQL
// still returns parse errors.
func TestIR_UtilityInvalidStatementsReturnError(t *testing.T) {
	tests := []string{
		"SET",
		"SET ;",
		"SET LOCAL",
		"SET SESSION",
		"SET =",
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

// TestIR_UtilityMultiStatementFirstStatementBehavior documents current parser
// contract: ParseSQL parses the first statement and ignores following ones.
func TestIR_UtilityMultiStatementFirstStatementBehavior(t *testing.T) {
	tests := []string{
		"SET client_min_messages = warning; SELECT 1",
		"SET log_min_messages = warning; SELECT 1",
		"SET client_min_messages = warning;;",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			assertUnknownUtility(t, sql)
		})
	}
}

// TestIR_UtilityFalsePositiveGuard ensures words that merely start with
// SET/SHOW/RESET are not treated as valid utility statements.
func TestIR_UtilityFalsePositiveGuard(t *testing.T) {
	_, err := ParseSQL("SETTINGS foo = bar")
	assert.Error(t, err)

	_, err = ParseSQL("SHOWCASE")
	assert.Error(t, err)

	_, err = ParseSQL("RESETTING")
	assert.Error(t, err)
}
