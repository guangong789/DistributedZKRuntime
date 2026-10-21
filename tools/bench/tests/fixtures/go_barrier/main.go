// Bounded noncryptographic fixture for the accepted Go start-barrier protocol.
package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"runtime"
	"time"
)

func run() error {
	if os.Getenv("DZKR_MIMCCHAIN_START_BARRIER") != "1" {
		return fmt.Errorf("barrier must be enabled")
	}
	path, token := os.Getenv("DZKR_MIMCCHAIN_BARRIER_SOCKET"), os.Getenv("DZKR_MIMCCHAIN_BARRIER_TOKEN")
	timeout, err := time.ParseDuration(os.Getenv("DZKR_MIMCCHAIN_BARRIER_TIMEOUT"))
	if err != nil || timeout <= 0 || timeout > 30*time.Second || path == "" || len(token) != 64 {
		return fmt.Errorf("invalid barrier configuration")
	}
	deadline := time.Now().Add(timeout)
	conn, err := net.DialTimeout("unix", path, timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err = conn.SetDeadline(deadline); err != nil {
		return err
	}
	encoder := json.NewEncoder(conn)
	if err = encoder.Encode(map[string]any{"version": 1, "event": "ready", "pid": os.Getpid(), "role": "probe", "token": token}); err != nil {
		return err
	}
	var release struct {
		Version int    `json:"version"`
		Event   string `json:"event"`
	}
	if err = json.NewDecoder(conn).Decode(&release); err != nil {
		return err
	}
	if release.Version != 1 || release.Event != "release" {
		return fmt.Errorf("invalid release")
	}
	if err = encoder.Encode(map[string]any{"version": 1, "event": "released", "pid": os.Getpid(), "role": "probe"}); err != nil {
		return err
	}
	data := make([]byte, 1024*1024)
	var sum uint64
	for i := range data {
		data[i] = byte(i)
		sum += uint64(data[i])
	}
	// Synthetic observation opportunity, not a benchmark timing closure.
	time.Sleep(300 * time.Millisecond)
	runtime.KeepAlive(data)
	return encoderOutput(sum)
}

func encoderOutput(sum uint64) error {
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"fixture": "g4-go-barrier-v1", "checksum": sum, "bytes": 1024 * 1024})
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "synthetic barrier failed:", err)
		os.Exit(1)
	}
}
