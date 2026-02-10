// Example: parsing CREATE, ALTER, and DELETE statements.
package main

import (
	"fmt"
	"log"

	"github.com/valkdb/postgresparser"
)

func main() {
	createSQL := `CREATE TABLE public.users (
    id integer NOT NULL,
    email text NOT NULL,
    name text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);`
	alterSQL := `ALTER TABLE public.users ADD COLUMN status text`
	deleteSQL := `DELETE FROM public.users WHERE id = 42`

	printParsed("CREATE TABLE", createSQL)
	printParsed("ALTER TABLE", alterSQL)
	printParsed("DELETE", deleteSQL)
}

func printParsed(label, sql string) {
	result, err := postgresparser.ParseSQL(sql)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("=== %s ===\n", label)
	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Command: %s\n", result.Command)
	for _, action := range result.DDLActions {
		if action.Schema != "" {
			fmt.Printf("Action: %s %s.%s\n", action.Type, action.Schema, action.ObjectName)
		} else {
			fmt.Printf("Action: %s %s\n", action.Type, action.ObjectName)
		}
		for _, col := range action.ColumnDetails {
			fmt.Printf("  - %s %s nullable=%t default=%q\n", col.Name, col.Type, col.Nullable, col.Default)
		}
	}
	if len(result.Where) > 0 {
		fmt.Printf("Where: %v\n", result.Where)
	}
	fmt.Println()
}
