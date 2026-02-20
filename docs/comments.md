# Comment Extraction Guide

This guide covers:

- `COMMENT ON ...` statement extraction (always enabled)
- inline `--` field comment extraction in `CREATE TABLE` (option-controlled)

## COMMENT ON (Always Enabled)

`COMMENT ON` statements are parsed as DDL actions with:

- `Type = "COMMENT"`
- `ObjectType` (for example `TABLE`, `COLUMN`, `INDEX`)
- `ObjectName`
- `Schema` (when available)
- `Target` (generic path form, for example `public.users.email`)
- `Comment`

### Example: table comment

```sql
COMMENT ON TABLE public.users IS 'Stores user account information';
```

### Example: column comment

```sql
COMMENT ON COLUMN public.users.email IS 'User email address, must be unique';
```

For column comments:

- `ObjectName` is the table name (`users`)
- `Columns` contains the target column (`["email"]`)

### Identifier Notes (`Target`, dots, quoting)

- `Target` preserves the SQL path text (for example `public.users.email`).
- Structured fields (`Schema`, `ObjectName`, `Columns`) are extracted with quote-aware splitting.
- Identifiers that contain `.` must be quoted in PostgreSQL SQL text (for example `public."my.table"."my.col"`).
- Unquoted dots are always treated as separators. For example, `public.my.table.my.col` is interpreted as a qualified path, not as identifier names containing dots.

Current behavior examples:
- `COMMENT ON COLUMN public."my.table"."my.col" IS 'x';`
  maps to `Schema=public`, `ObjectName="my.table"`, `Columns=["my.col"]`, `Target=public."my.table"."my.col"`.
- `COMMENT ON COLUMN public.my.table.my.col IS 'x';`
  maps to `Schema=public.my.table`, `ObjectName=my`, `Columns=["col"]`, `Target=public.my.table.my.col`.

### Example: index comment

```sql
COMMENT ON INDEX public.idx_bookings_dates IS 'Composite index for efficient date range queries on bookings';
```

### Supported Object Types

The following object types are fully classified (the `ObjectType` field is set to the exact type name):

- `TABLE`
- `COLUMN`
- `INDEX`
- `SCHEMA`
- `TYPE`
- `DOMAIN`
- `FOREIGN TABLE`
- `VIEW`
- `MATERIALIZED VIEW`
- `SEQUENCE`

The following object types produce `ObjectType: "UNKNOWN"`:

- `FUNCTION`
- `AGGREGATE`
- `OPERATOR`
- `CONSTRAINT`
- `PROCEDURE`
- `ROUTINE`
- `TRANSFORM`
- `OPERATOR CLASS`
- `OPERATOR FAMILY`
- `LARGE OBJECT`
- `CAST`

UNKNOWN types still parse the comment text correctly -- only the object type classification is degraded. The `Comment` field will contain the decoded comment string as expected. For example:

```sql
COMMENT ON FUNCTION public.my_func(integer) IS 'Does something';
```

produces `ObjectType: "UNKNOWN"` and `Comment: "Does something"`.

## CREATE TABLE Inline `--` Field Comments (Option-Controlled)

Inline field comments are disabled by default and enabled with:

- `ParseSQLWithOptions`
- `ParseSQLAllWithOptions`
- `ParseSQLStrictWithOptions`

using:

```go
postgresparser.ParseOptions{
    IncludeCreateTableFieldComments: true,
}
```

### Example SQL

```sql
CREATE TABLE public.users (
    -- [Attribute("Just an example")]
    -- required, min 5, max 55
    name text,

    -- single-column FK, inline
    org_id integer REFERENCES public.organizations(id)
);
```

### Behavior

- consecutive `--` lines immediately above a column are captured
- comments are trimmed and stored as `[]string`
- comments above table constraints are not attached to columns

### Performance Note

`IncludeCreateTableFieldComments` is opt-in because it performs extra hidden-token processing for `CREATE TABLE` column definitions.

In local benchmarks (`benchmark/bench_test.go`, `-benchmem`):
- non-DDL queries (`SELECT`) showed no meaningful allocation overhead
- `CREATE TABLE` parsing showed measurable overhead (roughly ~10-13% slower, with higher allocations)

Use this option only when you need inline `--` field comment metadata.

## Parser API Example

```go
opts := postgresparser.ParseOptions{
    IncludeCreateTableFieldComments: true,
}

res, err := postgresparser.ParseSQLWithOptions(sql, opts)
if err != nil {
    log.Fatal(err)
}

for _, action := range res.DDLActions {
    if action.Type == postgresparser.DDLComment {
        fmt.Println(action.ObjectType, action.Target, action.Comment)
    }
}
```

## Analysis API Example

The analysis layer reuses the same options type:

```go
opts := postgresparser.ParseOptions{
    IncludeCreateTableFieldComments: true,
}

res, err := analysis.AnalyzeSQLWithOptions(sql, opts)
if err != nil {
    log.Fatal(err)
}
```
