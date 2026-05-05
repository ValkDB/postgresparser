# Changelog

## Unreleased

### Added

- Placeholder syntactic-role exposure on `AnalyzeSQL` results.

### Improved

- JSONB `?&` and `?|` operators now lex as named compound tokens (`QUESTION_AND`, `QUESTION_OR`) for cleaner AST traversal, matching PostgreSQL's lexer behavior.

### Fixed

- `INTERVAL ?` in normalized SQL is now accepted by the grammar.
