# Supported SQL Statements

This document describes which PostgreSQL statement types are supported by the parser, what level of IR extraction they receive, and how unsupported or partially supported statements are handled.

## Fully Parsed Statements

These statements are walked via ANTLR visitors and produce rich IR metadata in `ParsedQuery`.

| Statement | `Command` | Key IR Sections |
|-----------|-----------|-----------------|
| `SELECT` | `SELECT` | `Columns`, `Tables`, `Where`, `GroupBy`, `Having`, `OrderBy`, `Limit`, `SetOperations`, `CTEs`, `Subqueries`, `ColumnUsage`, `DerivedColumns`, `Correlations` |
| `INSERT` | `INSERT` | `Tables`, `InsertColumns`, `Upsert`, `Returning`, `CTEs`, `ColumnUsage` |
| `UPDATE` | `UPDATE` | `Tables`, `SetClauses`, `Where`, `Returning`, `CTEs`, `ColumnUsage` |
| `DELETE` | `DELETE` | `Tables`, `Where`, `Returning`, `CTEs`, `ColumnUsage` |
| `MERGE` | `MERGE` | `Tables`, `Merge` (target, source, condition, actions) |
| `CREATE TABLE` | `DDL` | `Tables`, `DDLActions` (with `ColumnDetails`) |
| `ALTER TABLE` | `DDL` | `Tables`, `DDLActions` |
| `DROP TABLE` / `DROP INDEX` | `DDL` | `DDLActions` (with `Flags`) |
| `CREATE INDEX` | `DDL` | `DDLActions` (with `IndexType`) |
| `TRUNCATE` | `DDL` | `Tables`, `DDLActions` |

## Gracefully Handled (UNKNOWN) Statements

These statements are recognized by the parser and return `Command = "UNKNOWN"` with `RawSQL` populated. They do not produce parse errors, but no structured IR beyond the envelope is extracted.

| Statement | Notes |
|-----------|-------|
| `SET` | Includes `SET parameter = value`, `SET SESSION`, `SET LOCAL`. Values that are PL/pgSQL log-level tokens (`WARNING`, `NOTICE`, `DEBUG`, `INFO`, `EXCEPTION`, `ERROR`) are handled gracefully via error recovery. Common in `pg_dump` output. |
| `SHOW` | `SHOW parameter`, `SHOW ALL` |
| `RESET` | `RESET parameter`, `RESET ALL` |

### How graceful handling works

The ANTLR grammar does not fully cover all valid SET value tokens (for example `WARNING`, which is a reserved lexer token used in PL/pgSQL RAISE but not referenced in the parser rules for SET values).

- `SET <parameter> = <log level>` is recovered intentionally (returned as `QueryCommandUnknown` without a parse error) when:
  - `<log level>` is one of `WARNING`, `NOTICE`, `DEBUG`, `INFO`, `EXCEPTION`, or `ERROR`
  - parser errors point at the RHS log-level token (not elsewhere in the statement)
- This recovery also covers `SET SESSION` / `SET LOCAL` (single optional scope keyword), `=` / `TO`, and whitespace variants, and applies only to single statements with valid SET structure.
- All other `SET` / `SHOW` / `RESET` forms follow normal parse behavior: valid syntax returns `UNKNOWN`, invalid syntax returns `ParseErrors`.

## Unsupported Statements

Any SQL statement not listed above will either:

1. **Parse successfully** but return `Command = "UNKNOWN"` if ANTLR can parse the grammar without errors (the statement simply has no visitor implementation).
2. **Return a `ParseErrors`** if ANTLR cannot parse the grammar at all.

Examples of statements that currently return errors or UNKNOWN without structured extraction:

- `GRANT` / `REVOKE`
- `CREATE VIEW` / `CREATE FUNCTION` / `CREATE TRIGGER`
- `COPY`
- `EXPLAIN`
- `VACUUM` / `ANALYZE`
- `BEGIN` / `COMMIT` / `ROLLBACK`
- `LISTEN` / `NOTIFY`
- `COMMENT ON`
- `DO` (anonymous PL/pgSQL blocks)

## Adding Support for New Statements

See [architecture-decision-guide.md](architecture-decision-guide.md) for where new features belong (core parser vs analysis layer). To add a new fully-parsed statement type:

1. Add a new `QueryCommand` constant in `ir.go` (if needed).
2. Add a new visitor file (e.g., `grant.go`) or extend an existing one.
3. Add a `case` in the `switch` block in `entry.go`.
4. Add tests in a `parser_ir_*_test.go` file.
5. Update this document.
