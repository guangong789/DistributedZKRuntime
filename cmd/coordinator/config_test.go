package main

import (
	"flag"
	"io"
	"net"
	"testing"
	"time"
)

func TestCoordinatorExecutionConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		address string
		lease   time.Duration
	}{
		{"defaults", nil, ":50050", 5 * time.Second},
		{"deployment", []string{"-listen", "127.0.0.1:50050", "-attempt-lease", "45s"}, "127.0.0.1:50050", 45 * time.Second},
		{"IPv6", []string{"-listen", "[::1]:50050"}, "[::1]:50050", 5 * time.Second},
		{"hostname", []string{"-listen", "localhost:50050"}, "localhost:50050", 5 * time.Second},
		{"wildcard IPv4", []string{"-listen", "0.0.0.0:50050"}, "0.0.0.0:50050", 5 * time.Second},
		{"wildcard IPv6", []string{"-listen", "[::]:50050"}, "[::]:50050", 5 * time.Second},
		{"ephemeral", []string{"-listen", "127.0.0.1:0", "-attempt-lease", "50ms"}, "127.0.0.1:0", 50 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
			c := coordinatorExecutionFlags(fs)
			if err := fs.Parse(tc.args); err != nil {
				t.Fatal(err)
			}
			if err := c.validate(); err != nil || c.ListenAddress != tc.address || c.AttemptLease != tc.lease {
				t.Fatalf("configuration=%+v error=%v", c, err)
			}
		})
	}
}

func TestCoordinatorExecutionRejectsInvalidConfiguration(t *testing.T) {
	for _, args := range [][]string{
		{"-shutdown-timeout", "0"}, {"-shutdown-timeout", "-1s"}, {"-shutdown-timeout", "invalid"}, {"-shutdown-timeout", "9223372036854775808ns"},
		{"-attempt-lease", "0"}, {"-attempt-lease", "-1s"}, {"-attempt-lease", "invalid"}, {"-attempt-lease", "9223372036854775808ns"},
		{"-listen", ""}, {"-listen", "127.0.0.1"}, {"-listen", "localhost:"}, {"-listen", "localhost:65536"},
		{"-listen", "localhost:-1"}, {"-listen", "localhost:+1"}, {"-listen", "localhost:port"}, {"-listen", "localhost:050050"},
		{"-listen", "dns:///localhost:50050"}, {"-listen", "unix:///tmp/socket"}, {"-listen", "127.0.0.1:50050/path"},
		{"-listen", "::1:50050"}, {"-listen", "[localhost]:50050"}, {"-listen", "[127.0.0.1]:50050"},
		{"-listen", "[]:50050"}, {"-listen", "[]:0"},
		{"-listen", "[fe80::1%eth0]:50050"}, {"-listen", "worker name:50050"},
	} {
		t.Run(args[0]+"/"+args[1], func(t *testing.T) {
			fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			c := coordinatorExecutionFlags(fs)
			if err := fs.Parse(args); err == nil {
				if c.validate() == nil {
					t.Fatal("invalid configuration accepted")
				}
				if listener, err := c.listen(); err == nil || listener != nil {
					if listener != nil {
						listener.Close()
					}
					t.Fatal("invalid configuration created a listener")
				}
			}
		})
	}
}

func TestCoordinatorListenerBindsRequestedAddress(t *testing.T) {
	c := coordinatorExecutionConfig{ListenAddress: "127.0.0.1:0", AttemptLease: time.Second, ShutdownTimeout: defaultShutdownTimeout}
	listener, err := c.listen()
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	address := listener.Addr().(*net.TCPAddr)
	if !address.IP.Equal(net.ParseIP("127.0.0.1")) || address.Port == 0 {
		t.Fatalf("requested loopback bind changed: %v", address)
	}
}
