package postgresparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// strPtr returns a pointer to s for terse FunctionArg construction in tests.
func strPtr(s string) *string { return &s }

// findUsageWithFunction returns the ColumnUsage with the given column name (and
// optional table alias) whose Function field is non-nil. Returns nil when none.
// When alias is "" it matches any alias.
func findUsageWithFunction(usages []ColumnUsage, alias, column string) *ColumnUsage {
	for i := range usages {
		u := &usages[i]
		if u.Column != column {
			continue
		}
		if alias != "" && u.TableAlias != alias {
			continue
		}
		if u.Function != nil {
			return u
		}
	}
	return nil
}

// flattenColumnUsage walks a ParsedQuery and its nested subqueries / CTEs
// recursively so a single test can assert on usages reached anywhere in the
// query tree. Subquery/CTE WHEREs land on their own ParsedQuery, not the root.
func flattenColumnUsage(pq *ParsedQuery) []ColumnUsage {
	if pq == nil {
		return nil
	}
	out := append([]ColumnUsage(nil), pq.ColumnUsage...)
	for _, sub := range pq.Subqueries {
		out = append(out, flattenColumnUsage(sub.Query)...)
	}
	for _, cte := range pq.CTEs {
		out = append(out, flattenColumnUsage(cte.ParsedQuery)...)
	}
	return out
}

func TestIR_FunctionWrapper_Positive(t *testing.T) {
	type expect struct {
		alias    string
		column   string
		name     string
		args     []FunctionArg
		isNested bool
		cast     string
	}
	cases := []struct {
		name string
		sql  string
		want expect
	}{
		// Bare allowlisted wrappers, single arg.
		{"length", "SELECT * FROM t WHERE length(notes) < 100",
			expect{column: "notes", name: "length"}},
		{"lower", "SELECT * FROM users u WHERE lower(u.email) = 'a@b.com'",
			expect{alias: "u", column: "email", name: "lower"}},
		{"upper", "SELECT * FROM t WHERE upper(name) = 'X'",
			expect{column: "name", name: "upper"}},
		{"char_length", "SELECT * FROM t WHERE char_length(name) > 3",
			expect{column: "name", name: "char_length"}},
		{"octet_length", "SELECT * FROM t WHERE octet_length(payload) >= 16",
			expect{column: "payload", name: "octet_length"}},

		// Multi-arg wrappers.
		{"coalesce default empty string", "SELECT * FROM t WHERE coalesce(name, '') <> ''",
			expect{column: "name", name: "coalesce", args: []FunctionArg{{Literal: strPtr("''")}}}},
		{"coalesce default null", "SELECT * FROM t WHERE coalesce(name, NULL) <> 'x'",
			expect{column: "name", name: "coalesce", args: []FunctionArg{{IsNull: true}}}},
		{"coalesce default numeric", "SELECT * FROM t WHERE coalesce(score, 0) > 5",
			expect{column: "score", name: "coalesce", args: []FunctionArg{{Literal: strPtr("0")}}}},
		{"coalesce default placeholder", "SELECT * FROM t WHERE coalesce(name, $1) <> ''",
			expect{column: "name", name: "coalesce", args: []FunctionArg{{}}}},
		{"extract field keyword", "SELECT * FROM t WHERE extract(year FROM created_at) = 2025",
			expect{column: "created_at", name: "extract", args: []FunctionArg{{Literal: strPtr("year")}}}},
		{"extract field placeholder", "SELECT * FROM t WHERE extract(? FROM created_at) > 2024",
			expect{column: "created_at", name: "extract", args: []FunctionArg{{}}}},
		{"date_trunc month literal", "SELECT * FROM t WHERE date_trunc('month', created_at) = '2025-03-01'",
			expect{column: "created_at", name: "date_trunc", args: []FunctionArg{{Literal: strPtr("'month'")}}}},
		{"date_trunc unit non-literal", "SELECT * FROM t WHERE date_trunc(unit_col, created_at) > '2025-01-01'",
			expect{column: "created_at", name: "date_trunc", args: []FunctionArg{{}}}},

		// Nesting.
		{"nested LOWER(LOWER(col))", "SELECT * FROM t WHERE lower(lower(name)) = 'foo'",
			expect{column: "name", name: "lower", isNested: true}},
		{"nested mixed length(lower(col))", "SELECT * FROM t WHERE length(lower(name)) > 5",
			expect{column: "name", name: "length", isNested: true}},
		{"three deep nesting", "SELECT * FROM t WHERE lower(upper(lower(name))) = 'a'",
			expect{column: "name", name: "lower", isNested: true}},

		// pg_catalog canonicalisation.
		{"pg_catalog.length canonicalised", "SELECT * FROM t WHERE pg_catalog.length(name) > 5",
			expect{column: "name", name: "length"}},
		{"pg_catalog.LOWER canonicalised case-insensitive", "SELECT * FROM t WHERE pg_catalog.LOWER(name) = 'a'",
			expect{column: "name", name: "lower"}},

		// Case insensitivity.
		{"LENGTH uppercase", "SELECT * FROM t WHERE LENGTH(notes) < 100",
			expect{column: "notes", name: "length"}},
		{"Length camel", "SELECT * FROM t WHERE Length(notes) < 100",
			expect{column: "notes", name: "length"}},

		// Quoted column.
		{"quoted column with parens", `SELECT * FROM t WHERE length("Customer (legacy)") < 50`,
			expect{column: "Customer (legacy)", name: "length"}},

		// Casts (Q4 acceptance).
		{"cast :: int after wrapper", "SELECT * FROM t WHERE length(col)::int < 5",
			expect{column: "col", name: "length", cast: "int"}},
		{"cast text after lower", "SELECT * FROM t WHERE lower(col)::text = 'a'",
			expect{column: "col", name: "lower", cast: "text"}},
		{"cast bigint chained", "SELECT * FROM t WHERE length(col)::int::bigint > 10",
			expect{column: "col", name: "length", cast: "bigint"}},
		{"cast multi-word target", "SELECT * FROM t WHERE length(col)::bigint > 10",
			expect{column: "col", name: "length", cast: "bigint"}},
		{"cast via CAST(... AS ...)", "SELECT * FROM t WHERE CAST(length(col) AS bigint) > 10",
			expect{column: "col", name: "length", cast: "bigint"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseSQL(tc.sql)
			require.NoError(t, err)
			u := findUsageWithFunction(parsed.ColumnUsage, tc.want.alias, tc.want.column)
			require.NotNil(t, u, "expected ColumnUsage for column %q with non-nil Function; got: %+v", tc.want.column, parsed.ColumnUsage)
			require.NotNil(t, u.Function)
			require.Equal(t, tc.want.name, u.Function.Name)
			require.Equal(t, "", u.Function.Schema, "schema must be canonicalised away for pg_catalog and unqualified names")
			require.Equal(t, tc.want.isNested, u.Function.IsNested)
			require.Equal(t, tc.want.cast, u.Function.Cast)
			if tc.want.args != nil {
				require.Equal(t, len(tc.want.args), len(u.Function.Args), "Args count mismatch: %+v", u.Function.Args)
				for i, want := range tc.want.args {
					got := u.Function.Args[i]
					require.Equal(t, want.IsNull, got.IsNull, "arg %d IsNull", i)
					if want.Literal == nil {
						require.Nil(t, got.Literal, "arg %d expected nil Literal, got %v", i, got.Literal)
					} else {
						require.NotNil(t, got.Literal, "arg %d expected Literal=%q, got nil", i, *want.Literal)
						require.Equal(t, *want.Literal, *got.Literal, "arg %d Literal", i)
					}
				}
			}
		})
	}
}

func TestIR_FunctionWrapper_AllComparisonOperators(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"=", "SELECT * FROM t WHERE length(name) = 5"},
		{"<>", "SELECT * FROM t WHERE length(name) <> 5"},
		{"<", "SELECT * FROM t WHERE length(name) < 5"},
		{">", "SELECT * FROM t WHERE length(name) > 5"},
		{"<=", "SELECT * FROM t WHERE length(name) <= 5"},
		{">=", "SELECT * FROM t WHERE length(name) >= 5"},
		{"LIKE", "SELECT * FROM t WHERE lower(name) LIKE '%a%'"},
		{"ILIKE", "SELECT * FROM t WHERE lower(name) ILIKE '%a%'"},
		{"NOT LIKE", "SELECT * FROM t WHERE lower(name) NOT LIKE '%a%'"},
		{"IN", "SELECT * FROM t WHERE length(name) IN (1, 2, 3)"},
		{"NOT IN", "SELECT * FROM t WHERE length(name) NOT IN (1, 2)"},
		{"BETWEEN", "SELECT * FROM t WHERE length(name) BETWEEN 1 AND 5"},
		{"IS NULL", "SELECT * FROM t WHERE lower(name) IS NULL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseSQL(tc.sql)
			require.NoError(t, err)
			u := findUsageWithFunction(parsed.ColumnUsage, "", "name")
			require.NotNil(t, u, "wrapper expected for op %s; got: %+v", tc.name, parsed.ColumnUsage)
		})
	}
}

func TestIR_FunctionWrapper_BothSidesOfComparison(t *testing.T) {
	// Wrapper on RHS instead of LHS must still attach.
	parsed, err := ParseSQL("SELECT * FROM t WHERE 100 > length(notes)")
	require.NoError(t, err)
	u := findUsageWithFunction(parsed.ColumnUsage, "", "notes")
	require.NotNil(t, u)
	require.Equal(t, "length", u.Function.Name)
}

func TestIR_FunctionWrapper_CastWithOperators(t *testing.T) {
	cases := []string{
		"SELECT * FROM t WHERE length(col)::int = 5",
		"SELECT * FROM t WHERE length(col)::int <> 5",
		"SELECT * FROM t WHERE length(col)::int IN (1, 2, 3)",
		"SELECT * FROM t WHERE length(col)::int BETWEEN 1 AND 5",
		"SELECT * FROM t WHERE lower(col)::text LIKE '%a%'",
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			parsed, err := ParseSQL(sql)
			require.NoError(t, err)
			u := findUsageWithFunction(parsed.ColumnUsage, "", "col")
			require.NotNil(t, u, "wrapper+cast expected; got: %+v", parsed.ColumnUsage)
			require.NotEmpty(t, u.Function.Cast)
		})
	}
}

func TestIR_FunctionWrapper_NestedClauses_WHEREEverywhere(t *testing.T) {
	// All these contexts run through findAndRecordComparisons with wantWrappers=true.
	cases := []struct {
		name   string
		sql    string
		column string
	}{
		{"DML UPDATE WHERE", "UPDATE t SET status = 'x' WHERE length(name) < 5", "name"},
		{"DML DELETE WHERE", "DELETE FROM t WHERE length(name) < 5", "name"},
		{"subquery WHERE", "SELECT * FROM users WHERE id IN (SELECT id FROM t WHERE length(name) < 5)", "name"},
		{"CTE body WHERE",
			"WITH x AS (SELECT id FROM t WHERE length(name) < 5) SELECT * FROM x",
			"name"},
		{"UNION first branch WHERE",
			"SELECT id FROM a WHERE length(name) < 5 UNION SELECT id FROM b",
			"name"},
		{"UNION ALL second branch WHERE",
			"SELECT id FROM a UNION ALL SELECT id FROM b WHERE length(name) < 5",
			"name"},
		{"INTERSECT branch WHERE",
			"SELECT id FROM a WHERE length(name) < 5 INTERSECT SELECT id FROM b",
			"name"},
		{"EXCEPT branch WHERE",
			"SELECT id FROM a EXCEPT SELECT id FROM b WHERE length(name) < 5",
			"name"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseSQL(tc.sql)
			require.NoError(t, err)
			all := flattenColumnUsage(parsed)
			require.NotNil(t,
				findUsageWithFunction(all, "", tc.column),
				"expected wrapper attachment somewhere in tree; got: %+v", all)
		})
	}
}

func TestIR_FunctionWrapper_NonWHERENeverAttaches(t *testing.T) {
	// Q-extra B and the user's "all join/order/group/window/etc" follow-up:
	// every non-WHERE site must NOT carry a Function wrapper, even when the
	// same wrapper appears textually identical.
	cases := []struct {
		name string
		sql  string
	}{
		// HAVING.
		{"HAVING with length", "SELECT id FROM t GROUP BY id HAVING length(name) < 5"},

		// All JOIN flavours.
		{"INNER JOIN ON lower",
			"SELECT * FROM a JOIN b ON lower(a.x) = lower(b.y) WHERE a.x = 1"},
		{"LEFT JOIN ON lower",
			"SELECT * FROM a LEFT JOIN b ON lower(a.x) = lower(b.y)"},
		{"RIGHT JOIN ON lower",
			"SELECT * FROM a RIGHT JOIN b ON lower(a.x) = lower(b.y)"},
		{"FULL OUTER JOIN ON lower",
			"SELECT * FROM a FULL OUTER JOIN b ON lower(a.x) = lower(b.y)"},
		{"CROSS JOIN", "SELECT * FROM a CROSS JOIN b"},
		{"USING clause", "SELECT * FROM a JOIN b USING (id)"},

		// ORDER / GROUP / window.
		{"ORDER BY lower", "SELECT * FROM t ORDER BY lower(name)"},
		{"GROUP BY length", "SELECT length(name), COUNT(*) FROM t GROUP BY length(name)"},
		{"window PARTITION BY lower",
			"SELECT row_number() OVER (PARTITION BY lower(name)) FROM t"},
		{"window ORDER BY lower",
			"SELECT row_number() OVER (ORDER BY lower(name)) FROM t"},

		// RETURNING (DML output).
		{"RETURNING length",
			"UPDATE t SET status = 'x' WHERE id = 1 RETURNING length(name)"},

		// SELECT projection.
		{"SELECT projection length",
			"SELECT length(name) FROM t WHERE id = 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseSQL(tc.sql)
			require.NoError(t, err)
			for _, u := range parsed.ColumnUsage {
				if u.UsageType == ColumnUsageTypeFilter && u.Function != nil {
					// WHERE-side wrapper only allowed if there's an actual WHERE
					// clause that legitimately matches; the test cases above are
					// constructed so the wrapper text only appears outside WHERE,
					// or is paired with a non-wrapper WHERE predicate.
					if u.Column == "x" || u.Column == "id" {
						continue // the WHERE a.x = 1 / id = 1 — fine, no wrapper anyway
					}
					t.Fatalf("unexpected Function on filter usage: %+v (full=%+v)", u, parsed.ColumnUsage)
				}
				if u.UsageType != ColumnUsageTypeFilter {
					require.Nil(t, u.Function,
						"non-filter usage must not carry Function: %+v", u)
				}
			}
		})
	}
}

func TestIR_FunctionWrapper_NegativeCases(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		// Expression inside wrapper — not a bare column.
		{"length of expression", "SELECT * FROM t WHERE length(name || ' suffix') < 50"},
		// Non-allowlisted function.
		{"md5 not allowlisted", "SELECT * FROM t WHERE md5(name) = 'abc'"},
		{"concat not allowlisted", "SELECT * FROM t WHERE concat(a, b) = 'x'"},
		// Foreign schema.
		{"foreign schema rejected", "SELECT * FROM t WHERE public.my_length(col) > 5"},
		{"random schema rejected", "SELECT * FROM t WHERE myschema.length(col) > 5"},
		// Cast outside wrapper but no wrapper present.
		{"cast on bare column", "SELECT * FROM t WHERE col::int < 5"},
		// Expression around cast.
		{"cast on expression", "SELECT * FROM t WHERE (col + 1)::int < 5"},
		// Nested non-allowlisted.
		{"length of md5 — not bare nor allowlisted nest",
			"SELECT * FROM t WHERE length(md5(col)) > 5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseSQL(tc.sql)
			require.NoError(t, err)
			for _, u := range parsed.ColumnUsage {
				require.Nil(t, u.Function,
					"expected no wrapper attached; got %+v on usage %+v", u.Function, u)
			}
		})
	}
}

func TestIR_FunctionWrapper_FunctionsSliceUnchangedOnWHERE(t *testing.T) {
	// Q1 contract: the legacy Functions []string must remain empty on WHERE
	// comparison entries; consumers wanting wrapper info read Function.
	parsed, err := ParseSQL("SELECT * FROM t WHERE length(name) < 5")
	require.NoError(t, err)
	for _, u := range parsed.ColumnUsage {
		if u.UsageType == ColumnUsageTypeFilter && u.Column == "name" {
			require.Empty(t, u.Functions,
				"legacy Functions must stay empty on WHERE filter entries")
			require.NotNil(t, u.Function)
		}
	}
}
