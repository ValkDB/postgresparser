package benchmark

import (
	"testing"

	"github.com/antlr4-go/antlr/v4"
	pgquery "github.com/pganalyze/pg_query_go/v6"
	"github.com/valkdb/postgresparser"
	"github.com/valkdb/postgresparser/gen"
)

// queries covers a range of SQL complexity levels.
var queries = []struct {
	Name string
	SQL  string
}{
	{
		Name: "SimpleSelect",
		SQL:  `SELECT id, name, email FROM users WHERE active = true`,
	},
	{
		Name: "JoinThreeTables",
		SQL: `SELECT o.id, o.total, c.name, p.title
FROM orders o
JOIN customers c ON c.id = o.customer_id
JOIN products p ON p.id = o.product_id
WHERE o.created_at > '2024-01-01'
ORDER BY o.created_at DESC
LIMIT 100`,
	},
	{
		Name: "InsertUpsert",
		SQL: `INSERT INTO metrics (key, value, updated_at)
VALUES ('cpu_usage', 72.5, NOW()),
       ('mem_usage', 64.1, NOW()),
       ('disk_io', 120.3, NOW())
ON CONFLICT (key)
DO UPDATE SET value = EXCLUDED.value,
              updated_at = EXCLUDED.updated_at`,
	},
	{
		Name: "CTE",
		SQL: `WITH active_users AS (
    SELECT id, name FROM users WHERE active = true
),
recent_orders AS (
    SELECT user_id, COUNT(*) AS order_count
    FROM orders
    WHERE created_at > NOW() - INTERVAL '30 days'
    GROUP BY user_id
)
SELECT au.name, COALESCE(ro.order_count, 0) AS orders
FROM active_users au
LEFT JOIN recent_orders ro ON ro.user_id = au.id
ORDER BY orders DESC`,
	},
	{
		Name: "Subquery",
		SQL: `SELECT d.name, d.budget,
       (SELECT COUNT(*) FROM employees e WHERE e.department_id = d.id) AS headcount,
       (SELECT AVG(salary) FROM employees e WHERE e.department_id = d.id) AS avg_salary
FROM departments d
WHERE d.budget > (SELECT AVG(budget) FROM departments)
ORDER BY d.budget DESC`,
	},
	{
		Name: "DDL_CreateTable",
		SQL: `CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed BOOLEAN NOT NULL DEFAULT false
)`,
	},
	{
		Name: "DDL_CreateTable_Issue2",
		SQL: `CREATE TABLE public.users (
    id integer NOT NULL,
    email text NOT NULL,
    name text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
)`,
	},
	{
		Name: "ComplexUpdate",
		SQL: `UPDATE inventory
SET quantity = quantity - oi.qty,
    last_modified = NOW()
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
WHERE inventory.product_id = oi.product_id
  AND o.status = 'confirmed'
  AND inventory.quantity >= oi.qty`,
	},
	{
		Name: "WindowFunction",
		SQL: `SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank,
    salary - LAG(salary) OVER (PARTITION BY department ORDER BY salary DESC) AS gap,
    SUM(salary) OVER (PARTITION BY department) AS dept_total
FROM employees
WHERE hire_date >= '2020-01-01'`,
	},
}

// BenchmarkPostgresParser benchmarks the full ParseSQL pipeline (LL mode, default).
func BenchmarkPostgresParser(b *testing.B) {
	for _, q := range queries {
		b.Run(q.Name, func(b *testing.B) {
			result, err := postgresparser.ParseSQL(q.SQL)
			if err != nil {
				b.Fatalf("ParseSQL failed: %v", err)
			}
			if result == nil {
				b.Fatal("ParseSQL returned nil")
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				postgresparser.ParseSQL(q.SQL)
			}
		})
	}
}

// BenchmarkPgQueryGo benchmarks the cgo-based pg_query parser.
func BenchmarkPgQueryGo(b *testing.B) {
	for _, q := range queries {
		b.Run(q.Name, func(b *testing.B) {
			result, err := pgquery.Parse(q.SQL)
			if err != nil {
				b.Fatalf("pg_query.Parse failed: %v", err)
			}
			if result == nil {
				b.Fatal("pg_query.Parse returned nil")
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				pgquery.Parse(q.SQL)
			}
		})
	}
}

// BenchmarkANTLRParseOnly measures just the ANTLR lex+parse step (no IR extraction),
// to isolate the grammar cost from our Go code.
func BenchmarkANTLRParseOnly(b *testing.B) {
	for _, q := range queries {
		b.Run(q.Name, func(b *testing.B) {
			// Verify it parses.
			input := antlr.NewInputStream(q.SQL)
			lexer := gen.NewPostgreSQLLexer(input)
			stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
			parser := gen.NewPostgreSQLParser(stream)
			parser.BuildParseTrees = true
			parser.RemoveErrorListeners()
			root := parser.Root()
			if root == nil {
				b.Fatal("parse returned nil root")
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				input := antlr.NewInputStream(q.SQL)
				lexer := gen.NewPostgreSQLLexer(input)
				stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
				parser := gen.NewPostgreSQLParser(stream)
				parser.BuildParseTrees = true
				parser.RemoveErrorListeners()
				parser.Root()
			}
		})
	}
}

// BenchmarkANTLRParseOnly_SLL measures ANTLR lex+parse using SLL prediction mode.
// SLL is faster but may report false syntax errors on ambiguous grammars;
// the standard trick is to try SLL first and fall back to LL on error.
func BenchmarkANTLRParseOnly_SLL(b *testing.B) {
	for _, q := range queries {
		b.Run(q.Name, func(b *testing.B) {
			// Verify SLL mode works for this query.
			input := antlr.NewInputStream(q.SQL)
			lexer := gen.NewPostgreSQLLexer(input)
			stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
			parser := gen.NewPostgreSQLParser(stream)
			parser.BuildParseTrees = true
			parser.RemoveErrorListeners()
			parser.GetInterpreter().SetPredictionMode(antlr.PredictionModeSLL)
			errListener := &countErrorListener{}
			parser.AddErrorListener(errListener)
			root := parser.Root()
			if root == nil || errListener.count > 0 {
				b.Skipf("SLL mode fails for %s, skipping", q.Name)
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				input := antlr.NewInputStream(q.SQL)
				lexer := gen.NewPostgreSQLLexer(input)
				stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
				parser := gen.NewPostgreSQLParser(stream)
				parser.BuildParseTrees = true
				parser.RemoveErrorListeners()
				parser.GetInterpreter().SetPredictionMode(antlr.PredictionModeSLL)
				parser.Root()
			}
		})
	}
}

// BenchmarkANTLRParseOnly_SLLFallback measures the SLL-first, LL-fallback strategy.
func BenchmarkANTLRParseOnly_SLLFallback(b *testing.B) {
	for _, q := range queries {
		b.Run(q.Name, func(b *testing.B) {
			// Verify it parses.
			root := parseSLLWithFallback(q.SQL)
			if root == nil {
				b.Fatal("parse returned nil root")
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				parseSLLWithFallback(q.SQL)
			}
		})
	}
}

func parseSLLWithFallback(sql string) gen.IRootContext {
	input := antlr.NewInputStream(sql)
	lexer := gen.NewPostgreSQLLexer(input)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	parser := gen.NewPostgreSQLParser(stream)
	parser.BuildParseTrees = true
	parser.RemoveErrorListeners()

	// Try SLL first.
	parser.GetInterpreter().SetPredictionMode(antlr.PredictionModeSLL)
	errListener := &countErrorListener{}
	parser.AddErrorListener(errListener)
	root := parser.Root()
	if errListener.count == 0 {
		return root
	}

	// SLL failed — create fresh parser with LL mode.
	input = antlr.NewInputStream(sql)
	lexer = gen.NewPostgreSQLLexer(input)
	stream = antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	parser = gen.NewPostgreSQLParser(stream)
	parser.BuildParseTrees = true
	parser.RemoveErrorListeners()
	parser.GetInterpreter().SetPredictionMode(antlr.PredictionModeLL)
	return parser.Root()
}

// countErrorListener counts syntax errors without storing details.
type countErrorListener struct {
	antlr.DefaultErrorListener
	count int
}

func (l *countErrorListener) SyntaxError(_ antlr.Recognizer, _ interface{}, _, _ int, _ string, _ antlr.RecognitionException) {
	l.count++
}
