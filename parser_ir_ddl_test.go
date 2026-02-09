// parser_ir_ddl_test.go exercises DDL statement parsing at the IR level.
package postgresparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIR_DDL_DropTable(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantActions int
		wantType    DDLActionType
		wantObject  string
		wantFlags   []string
		wantTables  int
	}{
		{
			name:        "simple",
			sql:         "DROP TABLE users",
			wantActions: 1,
			wantType:    DDLDropTable,
			wantObject:  "users",
			wantTables:  1,
		},
		{
			name:        "IF EXISTS",
			sql:         "DROP TABLE IF EXISTS users",
			wantActions: 1,
			wantType:    DDLDropTable,
			wantObject:  "users",
			wantFlags:   []string{"IF_EXISTS"},
			wantTables:  1,
		},
		{
			name:        "CASCADE",
			sql:         "DROP TABLE users CASCADE",
			wantActions: 1,
			wantType:    DDLDropTable,
			wantObject:  "users",
			wantFlags:   []string{"CASCADE"},
			wantTables:  1,
		},
		{
			name:        "schema-qualified",
			sql:         "DROP TABLE myschema.users",
			wantActions: 1,
			wantType:    DDLDropTable,
			wantObject:  "myschema.users",
			wantTables:  1,
		},
		{
			name:        "multiple tables",
			sql:         "DROP TABLE users, orders, products",
			wantActions: 3,
			wantType:    DDLDropTable,
			wantTables:  3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ir := parseAssertNoError(t, tc.sql)
			assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
			require.Len(t, ir.DDLActions, tc.wantActions, "action count mismatch")

			act := ir.DDLActions[0]
			assert.Equal(t, tc.wantType, act.Type, "action type mismatch")
			if tc.wantObject != "" {
				assert.Equal(t, tc.wantObject, act.ObjectName, "object name mismatch")
			}
			assert.Subset(t, act.Flags, tc.wantFlags, "flags mismatch")
			assert.Len(t, ir.Tables, tc.wantTables, "tables count mismatch")
		})
	}
}

func TestIR_DDL_DropIndex(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantActions int
		wantFlags   []string
	}{
		{
			name:        "simple",
			sql:         "DROP INDEX idx_users_email",
			wantActions: 1,
		},
		{
			name:        "CONCURRENTLY",
			sql:         "DROP INDEX CONCURRENTLY idx_users_email",
			wantActions: 1,
			wantFlags:   []string{"CONCURRENTLY"},
		},
		{
			name:        "IF EXISTS",
			sql:         "DROP INDEX IF EXISTS idx_users_email",
			wantActions: 1,
			wantFlags:   []string{"IF_EXISTS"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ir := parseAssertNoError(t, tc.sql)
			assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
			require.Len(t, ir.DDLActions, tc.wantActions, "action count mismatch")

			act := ir.DDLActions[0]
			assert.Equal(t, DDLDropIndex, act.Type, "expected DROP_INDEX")
			assert.Equal(t, "idx_users_email", act.ObjectName, "object name mismatch")
			assert.Subset(t, act.Flags, tc.wantFlags, "flags mismatch")
		})
	}
}

func TestIR_DDL_CreateIndex(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantObject string
		wantCols   int
		wantFlags  []string
		wantIdx    string
		wantTables int
	}{
		{
			name:       "simple",
			sql:        "CREATE INDEX idx_email ON users (email)",
			wantObject: "idx_email",
			wantCols:   1,
			wantTables: 1,
		},
		{
			name:       "CONCURRENTLY",
			sql:        "CREATE INDEX CONCURRENTLY idx_email ON users (email)",
			wantObject: "idx_email",
			wantCols:   1,
			wantFlags:  []string{"CONCURRENTLY"},
			wantTables: 1,
		},
		{
			name:       "UNIQUE",
			sql:        "CREATE UNIQUE INDEX idx_email ON users (email)",
			wantObject: "idx_email",
			wantCols:   1,
			wantFlags:  []string{"UNIQUE"},
			wantTables: 1,
		},
		{
			name:       "USING gin",
			sql:        "CREATE INDEX idx_tags ON posts USING gin (tags)",
			wantObject: "idx_tags",
			wantCols:   1,
			wantIdx:    "gin",
			wantTables: 1,
		},
		{
			name:       "USING btree",
			sql:        "CREATE INDEX idx_name ON users USING btree (name)",
			wantObject: "idx_name",
			wantCols:   1,
			wantIdx:    "btree",
			wantTables: 1,
		},
		{
			name:       "multi-column",
			sql:        "CREATE INDEX idx_compound ON users (last_name, first_name)",
			wantObject: "idx_compound",
			wantCols:   2,
			wantTables: 1,
		},
		{
			name:       "IF NOT EXISTS",
			sql:        "CREATE INDEX IF NOT EXISTS idx_email ON users (email)",
			wantObject: "idx_email",
			wantCols:   1,
			wantFlags:  []string{"IF_NOT_EXISTS"},
			wantTables: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ir := parseAssertNoError(t, tc.sql)
			assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
			require.Len(t, ir.DDLActions, 1, "action count mismatch")

			act := ir.DDLActions[0]
			assert.Equal(t, DDLCreateIndex, act.Type, "expected CREATE_INDEX")
			assert.Equal(t, tc.wantObject, act.ObjectName, "object name mismatch")
			assert.Len(t, act.Columns, tc.wantCols, "column count mismatch")
			assert.Subset(t, act.Flags, tc.wantFlags, "flags mismatch")

			if tc.wantIdx != "" {
				assert.Equal(t, tc.wantIdx, act.IndexType, "index type mismatch")
			}
			assert.Len(t, ir.Tables, tc.wantTables, "tables count mismatch")
		})
	}
}

func TestIR_DDL_AlterTableDropColumn(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantCol   string
		wantFlags []string
	}{
		{
			name:    "simple",
			sql:     "ALTER TABLE users DROP COLUMN email",
			wantCol: "email",
		},
		{
			name:      "IF EXISTS",
			sql:       "ALTER TABLE users DROP COLUMN IF EXISTS email",
			wantCol:   "email",
			wantFlags: []string{"IF_EXISTS"},
		},
		{
			name:      "CASCADE",
			sql:       "ALTER TABLE users DROP COLUMN email CASCADE",
			wantCol:   "email",
			wantFlags: []string{"CASCADE"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ir := parseAssertNoError(t, tc.sql)
			assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
			require.Len(t, ir.DDLActions, 1, "action count mismatch")

			act := ir.DDLActions[0]
			assert.Equal(t, DDLDropColumn, act.Type, "expected DROP_COLUMN")
			require.Len(t, act.Columns, 1, "column count mismatch")
			assert.Equal(t, tc.wantCol, act.Columns[0], "column name mismatch")
			assert.Subset(t, act.Flags, tc.wantFlags, "flags mismatch")
			assert.True(t, containsTable(ir.Tables, "users"), "expected table 'users'")
		})
	}
}

func TestIR_DDL_AlterTableAddColumn(t *testing.T) {
	ir := parseAssertNoError(t, "ALTER TABLE users ADD COLUMN status text")
	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.DDLActions, 1, "action count mismatch")

	act := ir.DDLActions[0]
	assert.Equal(t, DDLAlterTable, act.Type, "expected ALTER_TABLE")
	require.Len(t, act.Columns, 1, "column count mismatch")
	assert.Equal(t, "status", act.Columns[0], "column mismatch")
	assert.Contains(t, act.Flags, "ADD_COLUMN", "expected flag ADD_COLUMN")
}

func TestIR_DDL_AlterTableMultiAction(t *testing.T) {
	ir := parseAssertNoError(t, "ALTER TABLE users ADD COLUMN status text, DROP COLUMN legacy")
	assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
	require.Len(t, ir.DDLActions, 2, "action count mismatch")

	// First action: ADD COLUMN
	assert.Equal(t, DDLAlterTable, ir.DDLActions[0].Type, "expected ALTER_TABLE for first action")
	assert.Contains(t, ir.DDLActions[0].Flags, "ADD_COLUMN", "expected flag ADD_COLUMN")

	// Second action: DROP COLUMN
	assert.Equal(t, DDLDropColumn, ir.DDLActions[1].Type, "expected DROP_COLUMN for second action")
	require.Len(t, ir.DDLActions[1].Columns, 1, "column count mismatch")
	assert.Equal(t, "legacy", ir.DDLActions[1].Columns[0], "column name mismatch")
}

func TestIR_DDL_Truncate(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantActions int
		wantFlags   []string
		wantTables  int
	}{
		{
			name:        "simple",
			sql:         "TRUNCATE users",
			wantActions: 1,
			wantTables:  1,
		},
		{
			name:        "TABLE keyword",
			sql:         "TRUNCATE TABLE users",
			wantActions: 1,
			wantTables:  1,
		},
		{
			name:        "CASCADE",
			sql:         "TRUNCATE TABLE users CASCADE",
			wantActions: 1,
			wantFlags:   []string{"CASCADE"},
			wantTables:  1,
		},
		{
			name:        "multiple tables",
			sql:         "TRUNCATE users, orders",
			wantActions: 2,
			wantTables:  2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ir := parseAssertNoError(t, tc.sql)
			assert.Equal(t, QueryCommandDDL, ir.Command, "expected DDL command")
			require.Len(t, ir.DDLActions, tc.wantActions, "action count mismatch")

			for _, act := range ir.DDLActions {
				assert.Equal(t, DDLTruncate, act.Type, "expected TRUNCATE type")
			}
			assert.Subset(t, ir.DDLActions[0].Flags, tc.wantFlags, "flags mismatch")
			assert.Len(t, ir.Tables, tc.wantTables, "tables count mismatch")
		})
	}
}
