package rpcauth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestPeerValidityCredentials(t *testing.T) {
	ca := newTestCA(t)
	cert := ca.issue(t, coordURI, nil)
	good := tls.ConnectionState{Version: tls.VersionTLS13, HandshakeComplete: true, VerifiedChains: [][]*x509.Certificate{{cert.pair.Leaf, ca.cert}}}
	guard := PeerValidityCredentials{}
	if !guard.RequireTransportSecurity() {
		t.Fatal("validity guard permitted plaintext")
	}
	for _, tc := range []struct {
		name string
		edit func(*tls.ConnectionState)
	}{
		{"valid", nil},
		{"unverified", func(s *tls.ConnectionState) { s.VerifiedChains = nil }},
		{"incomplete", func(s *tls.ConnectionState) { s.HandshakeComplete = false }},
		{"TLS12", func(s *tls.ConnectionState) { s.Version = tls.VersionTLS12 }},
		{"expired leaf", func(s *tls.ConnectionState) {
			expired := *cert.pair.Leaf
			expired.NotAfter = time.Now().Add(-time.Second)
			s.VerifiedChains = [][]*x509.Certificate{{&expired, ca.cert}}
		}},
		{"expired CA", func(s *tls.ConnectionState) {
			expired := *ca.cert
			expired.NotAfter = time.Now().Add(-time.Second)
			s.VerifiedChains = [][]*x509.Certificate{{cert.pair.Leaf, &expired}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := good
			if tc.edit != nil {
				tc.edit(&state)
			}
			ctx := credentials.NewContextWithRequestInfo(context.Background(), credentials.RequestInfo{AuthInfo: credentials.TLSInfo{State: state}})
			metadata, err := guard.GetRequestMetadata(ctx)
			if len(metadata) != 0 || (err == nil) != (tc.name == "valid") {
				t.Fatalf("metadata=%v error=%v", metadata, err)
			}
			if err != nil && status.Code(err) != codes.Unauthenticated {
				t.Fatalf("unexpected rejection: %v", err)
			}
		})
	}
	if _, err := guard.GetRequestMetadata(context.Background()); status.Code(err) != codes.Unauthenticated {
		t.Fatal("missing verified transport accepted")
	}
}
