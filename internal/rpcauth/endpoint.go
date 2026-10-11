package rpcauth

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// ParseTCPEndpoint accepts literal TCP host:port targets without rewriting them.
// DNS names must be ASCII (international names use explicit punycode). Resolver
// schemes, zones, alternate numeric IP encodings and unspecified hosts reject.
func ParseTCPEndpoint(address string) (string, error) {
	invalid := errors.New("invalid TCP host:port endpoint")
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || port == "" || len(port) > 5 || port[0] == '0' {
		return "", invalid
	}
	for _, c := range port {
		if c < '0' || c > '9' {
			return "", invalid
		}
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return "", invalid
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.Zone() != "" || ip.Unmap().IsUnspecified() || (ip.Is4() && strings.Contains(address, "[")) {
			return "", invalid
		}
		return host, nil
	}
	if len(host) > 253 || strings.ContainsAny(address, "[]") {
		return "", invalid
	}
	hasLetter := false
	allNumeric := true
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", invalid
		}
		if !numericAddressLabel(label) {
			allNumeric = false
		}
		for _, c := range label {
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
				hasLetter = true
			} else if (c < '0' || c > '9') && c != '-' {
				return "", invalid
			}
		}
	}
	if !hasLetter || allNumeric {
		return "", invalid
	}
	return host, nil
}

func numericAddressLabel(label string) bool {
	hex := strings.HasPrefix(label, "0x") || strings.HasPrefix(label, "0X")
	if hex {
		label = label[2:]
	}
	if label == "" {
		return false
	}
	for _, c := range label {
		if c >= '0' && c <= '9' {
			continue
		}
		if !hex || !((c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// VerifyPeerHostname checks an advertised host against the verified registering
// certificate, independently of the connection's source address or reverse DNS.
func VerifyPeerHostname(ctx context.Context, host string) error {
	if _, err := AuthenticatePeer(ctx); err != nil {
		return err
	}
	p, _ := peer.FromContext(ctx)
	info := p.AuthInfo.(credentials.TLSInfo)
	if err := info.State.VerifiedChains[0][0].VerifyHostname(host); err != nil {
		return status.Error(codes.PermissionDenied, "advertised host is not covered by peer certificate")
	}
	return nil
}
