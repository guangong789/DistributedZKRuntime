// Package testpki generates ephemeral credentials for localhost transport tests.
// It is imported only by tests; no deployment credentials or fixtures are used.
package testpki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	Coordinator = "spiffe://distributedzk.local/coordinator/main"
	Client      = "spiffe://distributedzk.local/client/client-a"
	Worker      = "spiffe://distributedzk.local/worker/worker-a"
)

type CA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

type Files struct {
	Cert, Key, CA string
	Pair          tls.Certificate
}

func New(t testing.TB) *CA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "temporary test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &CA{cert, key}
}

func (ca *CA) Roots() *x509.CertPool {
	roots := x509.NewCertPool()
	roots.AddCert(ca.cert)
	return roots
}

// Issue allows tests to model CA-signed malformed/expired credentials without
// weakening production loading. All files and keys are confined to t.TempDir.
func (ca *CA) Issue(t testing.TB, identity string, change func(*x509.Certificate)) Files {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(identity)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	leaf := &x509.Certificate{SerialNumber: serial, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		URIs: []*url.URL{u}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	if strings.Contains(identity, "/client/") {
		leaf.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	if change != nil {
		change(leaf)
	}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	dir := t.TempDir()
	f := Files{Cert: filepath.Join(dir, "cert.pem"), Key: filepath.Join(dir, "key.pem"), CA: filepath.Join(dir, "ca.pem")}
	for path, data := range map[string][]byte{
		f.Cert: certPEM, f.Key: keyPEM, f.CA: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw}),
	} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	f.Pair, err = tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return f
}
