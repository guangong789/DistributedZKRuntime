package main

import (
	"errors"
	"flag"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
)

const defaultAttemptLease = 5 * time.Second

type coordinatorExecutionConfig struct {
	ListenAddress string
	AttemptLease  time.Duration
}

func coordinatorExecutionFlags(fs *flag.FlagSet) *coordinatorExecutionConfig {
	c := &coordinatorExecutionConfig{}
	fs.StringVar(&c.ListenAddress, "listen", ":50050", "coordinator TCP listen address")
	fs.DurationVar(&c.AttemptLease, "attempt-lease", defaultAttemptLease, "positive per-attempt lease duration")
	return c
}

func (c coordinatorExecutionConfig) validate() error {
	if c.AttemptLease <= 0 {
		return errors.New("-attempt-lease must be positive")
	}
	return validateCoordinatorListenAddress(c.ListenAddress)
}

// Binding differs from advertising: wildcard hosts and port 0 (ephemeral) are
// valid listeners. Resolver URLs, service names and scoped IPv6 are not accepted.
func validateCoordinatorListenAddress(address string) error {
	invalid := errors.New("-listen must be a TCP host:port address with a numeric port from 0 to 65535")
	host, port, err := net.SplitHostPort(address)
	if err != nil || len(port) == 0 || len(port) > 5 || (len(port) > 1 && port[0] == '0') {
		return invalid
	}
	for _, c := range port {
		if c < '0' || c > '9' {
			return invalid
		}
	}
	n, err := strconv.Atoi(port)
	if err != nil || n > 65535 {
		return invalid
	}
	if host == "" {
		if strings.HasPrefix(address, "[") {
			return invalid
		}
		return nil
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.Zone() != "" || (ip.Is4() && strings.HasPrefix(address, "[")) {
			return invalid
		}
		return nil
	}
	if strings.HasPrefix(address, "[") {
		return invalid
	}
	// Reuse the existing strict DNS syntax check with a valid dummy port.
	if _, err := rpcauth.ParseTCPEndpoint(net.JoinHostPort(host, "1")); err != nil {
		return invalid
	}
	return nil
}

func (c coordinatorExecutionConfig) listen() (net.Listener, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	return net.Listen("tcp", c.ListenAddress)
}
