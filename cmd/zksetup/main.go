package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/guangong789/DistributedZKRuntime/internal/zk"
)

func main() {
	outDir := flag.String(
		"out",
		"zk-artifacts/square",
		"directory for square circuit setup artifacts",
	)

	flag.Parse()

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("create output directory: %v", err)
	}

	pkPath := filepath.Join(*outDir, "proving.key")
	vkPath := filepath.Join(*outDir, "verifying.key")

	if err := zk.GenerateSquareSetup(pkPath, vkPath); err != nil {
		log.Fatalf("generate square setup: %v", err)
	}

	fmt.Printf("proving key: %s\n", pkPath)
	fmt.Printf("verifying key: %s\n", vkPath)
}
