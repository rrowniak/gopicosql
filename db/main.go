package main

import (
	"fmt"
	"os"

	"gopicosql/db/engine"

	"gopicosql/db/rest"
)

func printHelp() {
	fmt.Printf("Usage: %s <CONFIG_FILE>\n", os.Args[0])
}

func main() {
	if len(os.Args) > 2 {
		printHelp()
		os.Exit(1)
	}

	var cfg *engine.Cfg

	if len(os.Args) == 1 {
		cfg = engine.NewConfigDefault()
	} else {
		cfg = engine.NewConfig(os.Args[1])
	}

	s, err := rest.NewServer(cfg)

	if err != nil {
		panic(err)
	}

	s.Run()
}
