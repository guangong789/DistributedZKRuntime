package rpcauth

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/url"
	"os"
	"slices"
	"time"

	"google.golang.org/grpc/credentials"
)

// Material holds validated local credentials and an explicit application trust
// bundle. Load once before serving; files are not watched or reloaded.
type Material struct {
	certificate tls.Certificate
	roots       *x509.CertPool
	principal   Principal
}

// LoadMaterial verifies the key pair, chain, local identity, validity and every
// required EKU. Only explicitly supplied CA certificates become trust anchors;
// system roots are never used. Errors omit paths and certificate contents.
func LoadMaterial(certFile, keyFile, caFile string, expected Principal) (*Material, error) {
	if !expected.valid() {
		return nil, errors.New("valid local principal required")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, errors.New("cannot load certificate/key pair")
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, errors.New("cannot read trust bundle")
	}
	roots, err := parseTrustBundle(caPEM)
	if err != nil {
		return nil, err
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, errors.New("invalid local certificate")
	}
	principal, err := certificatePrincipal(leaf)
	if err != nil || principal != expected {
		return nil, errors.New("local certificate identity mismatch")
	}
	if err := checkRoleUsages(leaf, principal); err != nil {
		return nil, err
	}
	intermediates := x509.NewCertPool()
	for _, der := range cert.Certificate[1:] {
		intermediate, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, errors.New("invalid local certificate chain")
		}
		intermediates.AddCert(intermediate)
	}
	// VerifyOptions.KeyUsages is OR, so verify separately to require both
	// ServerAuth and ClientAuth throughout a Worker/Coordinator's chain.
	for _, usage := range roleUsages(principal.Role()) {
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
			return nil, errors.New("local certificate chain or validity verification failed")
		}
	}
	cert.Leaf = leaf
	return &Material{certificate: cert, roots: roots, principal: principal}, nil
}

func parseTrustBundle(data []byte) (*x509.CertPool, error) {
	roots := x509.NewCertPool()
	count := 0
	for len(bytes.TrimSpace(data)) != 0 {
		data = bytes.TrimSpace(data)
		if !bytes.HasPrefix(data, []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, errors.New("invalid CA trust bundle")
		}
		block, rest := pem.Decode(data)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, errors.New("invalid CA trust bundle")
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.BasicConstraintsValid || !cert.IsCA || cert.KeyUsage&x509.KeyUsageCertSign == 0 {
			return nil, errors.New("trust bundle must contain CA certificates")
		}
		roots.AddCert(cert)
		count++
		data = rest
	}
	if count == 0 {
		return nil, errors.New("empty CA trust bundle")
	}
	return roots, nil
}

func roleUsages(role Role) []x509.ExtKeyUsage {
	if role == Client {
		return []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	return []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}
}

func checkRoleUsages(cert *x509.Certificate, principal Principal) error {
	if cert.IsCA {
		return errors.New("application identity requires a leaf certificate")
	}
	for _, usage := range roleUsages(principal.Role()) {
		// No implicit Any/absent-EKU permission for application credentials.
		if !slices.Contains(cert.ExtKeyUsage, usage) {
			return errors.New("certificate lacks required role usage")
		}
	}
	return nil
}

func (m *Material) valid() bool {
	return m != nil && m.principal.valid() && m.roots != nil && m.certificate.Leaf != nil
}

// NewServerTLS requires mTLS and admits only recognized certificate identities.
// Pair it with Policy.UnaryInterceptor to authorize individual methods.
func NewServerTLS(local *Material, policy *Policy) (*tls.Config, error) {
	if !local.valid() || local.principal.Role() == Client || !policy.recognizes(local.principal) {
		return nil, errors.New("recognized server credentials required")
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{local.certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    local.roots.Clone(),
		NextProtos:   []string{"h2"},
		VerifyConnection: func(state tls.ConnectionState) error {
			principal, err := verifiedPrincipal(state, time.Now())
			if err != nil {
				return err
			}
			if !policy.recognizes(principal) {
				return errors.New("unrecognized peer identity")
			}
			return nil
		},
	}, nil
}

// NewClientTLS presents local credentials and requires BOTH normal DNS/IP SAN
// verification and the exact expected server URI. serverName is the trusted dial
// hostname/IP or an explicit operator-configured override, never a peer claim.
// There is no plaintext or anonymous-client fallback.
func NewClientTLS(local *Material, serverName string, expected Principal) (*tls.Config, error) {
	if !local.valid() || serverName == "" || !expected.valid() || expected.Role() == Client {
		return nil, errors.New("local credentials, server name and server principal required")
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{local.certificate},
		RootCAs:      local.roots.Clone(),
		ServerName:   serverName,
		NextProtos:   []string{"h2"},
		VerifyConnection: func(state tls.ConnectionState) error {
			principal, err := verifiedPrincipal(state, time.Now())
			if err != nil {
				return err
			}
			if principal != expected {
				return errors.New("server identity mismatch")
			}
			return nil
		},
	}, nil
}

// WorkerCredentials creates fresh TLS credentials for one selected Worker.
// Both host and logical identity come from that attempt's registry snapshot;
// responses and peer metadata cannot change the expectation. No connection is
// cached and no certificate file is reloaded per attempt.
func (m *Material) WorkerCredentials(workerID, address string) (credentials.TransportCredentials, error) {
	if !m.valid() || m.principal.Role() != Coordinator {
		return nil, errors.New("Coordinator dispatch credentials required")
	}
	host, err := ParseTCPEndpoint(address)
	if err != nil {
		return nil, err
	}
	expected, err := ParsePrincipal("spiffe://distributedzk.local/worker/" + url.PathEscape(workerID))
	if err != nil {
		return nil, errors.New("invalid selected Worker identity")
	}
	config, err := NewClientTLS(m, host, expected)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(config), nil
}
