package main

import (
	"os"

	"github.com/kapitanov/natandb/cmd"
)

func main() {
	cmd.Main(os.Args[1:])
}
