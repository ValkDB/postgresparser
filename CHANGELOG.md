# Changelog

## Unreleased

### Added

- Placeholder syntactic-role exposure on `AnalyzeSQL` results.
- `ColumnUsage.Function` (type `*FunctionWrapper`) on WHERE-clause predicates: captures the wrapping function name, literal arguments, an `IsNested` flag and an outermost-cast target type for the eight allowlisted wrappers `length`, `lower`, `upper`, `coalesce`, `extract`, `date_trunc`, `char_length`, `octet_length`. `pg_catalog.<name>` is canonicalised; any other schema rejects the wrapper. Mirrored on `SQLColumnUsage` via the `SQLFunctionWrapper` / `SQLFunctionArg` aliases. Out of scope for v1 and explicit non-goals: wrappers around expressions (`length(col || 'x')`), non-allowlisted functions, and non-WHERE-equivalent locations (HAVING, JOIN ON, ORDER BY, GROUP BY, window PARTITION/ORDER, RETURNING, projection) — these never attach a wrapper.

### Improved

- JSONB `?&` and `?|` operators now lex as named compound tokens (`QUESTION_AND`, `QUESTION_OR`) for cleaner AST traversal, matching PostgreSQL's lexer behavior.

### Fixed

- `INTERVAL ?` in normalized SQL is now accepted by the grammar.
