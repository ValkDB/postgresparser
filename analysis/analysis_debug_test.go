package analysis

import (
	"testing"
)

// TestDebugOperatorExtraction logs operator extraction details for compound WHERE conditions.
func TestDebugOperatorExtraction(t *testing.T) {
	sql := `SELECT * FROM products WHERE price > 100 AND stock <= 50`

	result, err := AnalyzeSQL(sql)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Column Usage:")
	for i, usage := range result.ColumnUsage {
		t.Logf("%d. Column: %s, Operator: %s, Type: %s, Context: %.50s",
			i+1, usage.Column, usage.Operator, usage.UsageType, usage.Context)
	}
}
