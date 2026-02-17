package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/valkdb/postgresparser"
)

func main() {
	sql := `
SELECT 1;
SELECT FROM;
SELECT 2;
`

	batch, err := postgresparser.ParseSQLAll(sql)
	if err != nil {
		log.Fatalf("parse failed: %v", err)
	}

	fmt.Printf("statements=%d has_failures=%t\n", len(batch.Statements), batch.HasFailures)
	for _, stmt := range batch.Statements {
		failed := stmt.Query == nil
		command := "<nil>"
		if stmt.Query != nil {
			command = string(stmt.Query.Command)
		}

		warnCodes := make([]string, 0, len(stmt.Warnings))
		for _, w := range stmt.Warnings {
			warnCodes = append(warnCodes, string(w.Code))
		}

		fmt.Printf(
			"statement=%d failed=%t command=%s warnings=%s raw=%q\n",
			stmt.Index,
			failed,
			command,
			strings.Join(warnCodes, ","),
			stmt.RawSQL,
		)
	}
}
