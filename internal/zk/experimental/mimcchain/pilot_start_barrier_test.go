package mimcchain

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

// Opt-in experiment synchronization only. Both callers invoke this before any
// fixture creation or benchmark clock. No wrapper/exec RSS is passed off as Go RSS.
func pilotStartBarrier(role string) error {
	if os.Getenv("DZKR_MIMCCHAIN_START_BARRIER") != "1" {
		return nil
	}
	path, token := os.Getenv("DZKR_MIMCCHAIN_BARRIER_SOCKET"), os.Getenv("DZKR_MIMCCHAIN_BARRIER_TOKEN")
	timeout := 5 * time.Second
	if value := os.Getenv("DZKR_MIMCCHAIN_BARRIER_TIMEOUT"); value != "" {
		var err error
		timeout, err = time.ParseDuration(value)
		if err != nil || timeout <= 0 || timeout > 30*time.Second {
			return errors.New("invalid experimental barrier timeout")
		}
	}
	if path == "" || len(token) != 64 {
		return errors.New("experimental barrier not configured")
	}
	start := time.Now()
	deadline := start.Add(timeout)
	conn, err := net.DialTimeout("unix", path, timeout)
	if err != nil {
		return errors.New("experimental barrier connection failed")
	}
	defer conn.Close()
	if conn.SetDeadline(deadline) != nil {
		return errors.New("experimental barrier deadline failed")
	}
	encoder := json.NewEncoder(conn)
	if encoder.Encode(map[string]any{"version": 1, "event": "ready", "pid": os.Getpid(), "role": role, "token": token}) != nil {
		return errors.New("experimental barrier readiness failed")
	}
	var release struct {
		Version int    `json:"version"`
		Event   string `json:"event"`
	}
	if json.NewDecoder(conn).Decode(&release) != nil || release.Version != 1 || release.Event != "release" {
		return errors.New("experimental barrier release failed")
	}
	if encoder.Encode(map[string]any{"version": 1, "event": "released", "pid": os.Getpid(), "role": role}) != nil {
		return errors.New("experimental barrier acknowledgement failed")
	}
	fmt.Printf("MIMCCHAIN_BARRIER {\"role\":%q,\"startup_sync_ns\":%d}\n", role, time.Since(start).Nanoseconds())
	return nil
}

func TestPilotStartBarrierDisabled(t *testing.T) {
	t.Setenv("DZKR_MIMCCHAIN_START_BARRIER", "")
	t.Setenv("DZKR_MIMCCHAIN_BARRIER_SOCKET", "/must-not-connect")
	if err := pilotStartBarrier("probe"); err != nil {
		t.Fatal(err)
	}
}

// Synthetic CLI endpoint exercises the exact hook without compiling a circuit,
// creating a witness, or calling any cryptographic operation.
func TestMiMCChainStartBarrierProbe(t *testing.T) {
	if os.Getenv("DZKR_MIMCCHAIN_BARRIER_PROBE") != "1" {
		t.Skip("opt-in non-cryptographic start-barrier probe")
	}
	mode := os.Getenv("DZKR_MIMCCHAIN_BARRIER_PROBE_MODE")
	if mode == "preexit" {
		t.Fatal("controlled exit before handshake")
	}
	if err := pilotStartBarrier("probe"); err != nil {
		t.Fatal(err)
	}
	fmt.Println("MIMCCHAIN_BARRIER_PROBE_WORK")
	switch mode {
	case "nonzero":
		t.Fatal("controlled nonzero after release")
	case "hold":
		for {
			time.Sleep(time.Hour) // Synthetic post-release wait; never measurement code.
		}
	case "trivial":
		if len([]byte("ready")) != 5 {
			t.Fatal("trivial work failed")
		}
	}
}
