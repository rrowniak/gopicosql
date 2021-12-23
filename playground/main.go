package main

import (
	"fmt"

	"github.com/rrowniak/sqlparser"
)

func main() {
	actual, err := sqlparser.ParseMany([]string{"select * from db"})
	if err != nil {
		panic(err)
	}
	fmt.Println(actual)
}
