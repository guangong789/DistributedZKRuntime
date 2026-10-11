package rpcauth

import (
	"context"
	"testing"
)

func TestParseTCPEndpoint(t *testing.T) {
	for _, tc := range []struct{ address, host string }{
		{"127.0.0.1:50051", "127.0.0.1"}, {"localhost:1", "localhost"}, {"worker.example:65535", "worker.example"},
		{"worker-1:1234", "worker-1"}, {"Worker.EXAMPLE:1234", "Worker.EXAMPLE"}, {"xn--bcher-kva.example:1234", "xn--bcher-kva.example"},
		{"[::1]:1234", "::1"}, {"[2001:db8::1]:443", "2001:db8::1"}, {"[0:0:0:0:0:0:0:1]:443", "0:0:0:0:0:0:0:1"},
	} {
		t.Run(tc.address, func(t *testing.T) {
			host, err := ParseTCPEndpoint(tc.address)
			if err != nil || host != tc.host {
				t.Fatalf("endpoint rejected or rewritten: %v", err)
			}
		})
	}
	for _, address := range []string{
		"", "worker", ":1234", "worker:", "worker:0", "worker:65536", "worker:-1", "worker:+80", "worker:080", "worker:port", "worker:999999999999999999999999",
		"dns:///worker:1234", "unix:///tmp/socket", "passthrough:///worker:1234", "https://worker:1234", "worker:1234/path", "worker:1234?x=1",
		"user@worker:1234", "worker%2eexample:1234", "worker.example.:1234", "worker..example:1234", "-worker:1234", "worker-:1234",
		"worker_name:1234", "worker example:1234", " worker:1234", "worker:1234 ", "worker\\example:1234", "[worker]:1234",
		"127.00.0.1:1234", "2130706433:1234", "0177.0.0.1:1234", "0x7f000001:1234", "0X7f.0.0.1:1234", "0xffffffffffffffffffff:1234", "[127.0.0.1]:1234",
		"0.0.0.0:1234", "[::]:1234", "[::ffff:0.0.0.0]:1234", "[fe80::1%eth0]:1234", "::1:1234", "[::1]1234", "[::1]:1234:55",
		"工作.example:1234", "worker\x00.example:1234",
	} {
		t.Run("reject/"+address, func(t *testing.T) {
			if _, err := ParseTCPEndpoint(address); err == nil {
				t.Fatal("unsafe endpoint accepted")
			}
		})
	}
	if err := VerifyPeerHostname(context.Background(), "localhost"); err == nil {
		t.Fatal("unverified context bound to advertised hostname")
	}
}

func TestWorkerTransportMissingMaterialFailsClosed(t *testing.T) {
	var material *Material
	if transport, err := material.WorkerCredentials("worker-a", "localhost:1234"); err == nil || transport != nil {
		t.Fatal("missing dispatch material accepted")
	}
	if server, err := NewWorkerServer(nil, principal(t, coordURI), nil); err == nil || server != nil {
		t.Fatal("missing Worker server material accepted")
	}
}
