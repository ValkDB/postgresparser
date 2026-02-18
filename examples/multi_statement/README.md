# Multi-Statement Correlation Example

This example shows how to use `ParseSQLAll` to correlate parser output back to each input statement.

## Why this shape

`ParseSQLAll` returns one entry per input statement:

- `Statements[i].Index` is the 1-based statement position.
- `Statements[i].RawSQL` is that statement's SQL text.
- `Statements[i].Query` is the parsed IR (`nil` means that statement failed IR conversion).
- `Statements[i].Warnings` are statement-scoped warnings (currently `SYNTAX_ERROR`).
- `HasFailures` is `true` when any statement has a nil `Query` or any `Warnings`.

This gives deterministic correlation for mixed-success batches.

## Example input

```sql
SELECT 1;
SELECT FROM;
SELECT 2;
```

Typical outcome:

- Statement 1: `Query != nil`, `Warnings=[]`
- Statement 2: `Query != nil`, `Warnings=[SYNTAX_ERROR]`
- Statement 3: `Query != nil`, `Warnings=[]`

## Run

```bash
go run ./examples/multi_statement
```

The sample program prints per-statement status and warning codes so you can see exactly what passed or failed.
