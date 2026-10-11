// Package rpcauth provides mTLS identity and unary RPC authorization.
package rpcauth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/asn1"
	"errors"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type Role string

const (
	Coordinator Role = "coordinator"
	Worker      Role = "worker"
	Client      Role = "client"
)

// Principal is immutable outside this package. Parsing a configured identity
// does not authenticate a caller; use AuthenticatePeer for request contexts.
type Principal struct {
	role Role
	id   string
}

func (p Principal) Role() Role { return p.role }
func (p Principal) ID() string { return p.id }
func (p Principal) WorkerID() string {
	if p.role == Worker {
		return p.id
	}
	return ""
}
func (p Principal) URI() string {
	return "spiffe://distributedzk.local/" + string(p.role) + "/" + url.PathEscape(p.id)
}

// ParsePrincipal accepts exactly one canonical URI spelling. IDs are opaque,
// nonempty UTF-8 strings, encoded with url.PathEscape as a single path segment.
// This preserves existing WorkerIDs (including reserved characters), without
// case folding, path cleaning, or treating an escaped slash as a separator.
// These are application identities, not an implementation of the SPIFFE spec.
func ParsePrincipal(identity string) (Principal, error) {
	u, err := url.Parse(identity)
	if err != nil || u.Scheme != "spiffe" || u.Host != "distributedzk.local" ||
		u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(identity, "#") {
		return Principal{}, errors.New("invalid principal URI")
	}
	parts := strings.Split(u.EscapedPath(), "/")
	if len(parts) != 3 || parts[0] != "" {
		return Principal{}, errors.New("invalid principal path")
	}
	id, err := url.PathUnescape(parts[2])
	if err != nil || id == "" || !utf8.ValidString(id) {
		return Principal{}, errors.New("invalid principal ID")
	}
	p := Principal{role: Role(parts[1]), id: id}
	if !p.valid() || p.URI() != identity {
		return Principal{}, errors.New("invalid or noncanonical principal URI")
	}
	return p, nil
}

func (p Principal) valid() bool {
	if p.id == "" || !utf8.ValidString(p.id) {
		return false
	}
	switch p.role {
	case Coordinator:
		return p.id == "main"
	case Worker, Client:
		return true
	default:
		return false
	}
}

func certificatePrincipal(cert *x509.Certificate) (Principal, error) {
	if cert == nil || len(cert.URIs) != 1 || cert.URIs[0] == nil {
		return Principal{}, errors.New("certificate must contain one principal URI")
	}
	// x509's parsed URL can discard spelling information (e.g. upper-case
	// schemes or an empty fragment). Validate the original SAN URI bytes too.
	for _, ext := range cert.Extensions {
		if !ext.Id.Equal(asn1.ObjectIdentifier{2, 5, 29, 17}) {
			continue
		}
		var names []asn1.RawValue
		rest, err := asn1.Unmarshal(ext.Value, &names)
		if err != nil || len(rest) != 0 {
			break
		}
		for _, name := range names {
			if name.Class == asn1.ClassContextSpecific && name.Tag == 6 && !name.IsCompound {
				return ParsePrincipal(string(name.Bytes))
			}
		}
	}
	return Principal{}, errors.New("certificate principal SAN unavailable")
}

func verifiedPrincipal(state tls.ConnectionState, now time.Time) (Principal, error) {
	if state.Version < tls.VersionTLS13 ||
		len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return Principal{}, errors.New("verified TLS 1.3 peer required")
	}
	// Check validity on each RPC as well as during the handshake. A long-lived
	// connection must not keep authenticating after its verified chain expires.
	chain := state.VerifiedChains[0]
	for _, cert := range chain {
		if cert == nil || now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
			return Principal{}, errors.New("peer certificate is not currently valid")
		}
	}
	p, err := certificatePrincipal(chain[0])
	if err != nil {
		return Principal{}, err
	}
	if err := checkRoleUsages(chain[0], p); err != nil {
		return Principal{}, err
	}
	return p, nil
}

// AuthenticatePeer trusts only the chain verified by gRPC's TLS transport.
// It neither uses metadata/CN nor grants permission to invoke a method.
func AuthenticatePeer(ctx context.Context) (Principal, error) {
	p, ok := peer.FromContext(ctx)
	if ok && p != nil {
		if info, ok := p.AuthInfo.(credentials.TLSInfo); ok && info.State.HandshakeComplete {
			if principal, err := verifiedPrincipal(info.State, time.Now()); err == nil {
				return principal, nil
			}
		}
	}
	return Principal{}, status.Error(codes.Unauthenticated, "verified peer identity required")
}
