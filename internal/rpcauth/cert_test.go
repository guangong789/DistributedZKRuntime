package rpcauth

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
	"testing"
	"time"
)

const coordURI = "spiffe://distributedzk.local/coordinator/main"
const workerURI = "spiffe://distributedzk.local/worker/worker-a"
const workerBURI = "spiffe://distributedzk.local/worker/worker-b"
const clientURI = "spiffe://distributedzk.local/client/client-a"

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}
type testCertificate struct {
	pair            tls.Certificate
	certPEM, keyPEM []byte
}

func principal(t *testing.T, uri string) Principal {
	t.Helper()
	p, err := ParsePrincipal(uri)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func testPolicy(t *testing.T) *Policy {
	t.Helper()
	p, err := NewPolicy(principal(t, coordURI), []Principal{principal(t, clientURI)}, []Principal{principal(t, workerURI), principal(t, workerBURI)})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func newTestCA(t *testing.T) testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return testCA{cert, key}
}

func (ca testCA) issue(t *testing.T, uri string, change func(*x509.Certificate)) testCertificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "not-an-identity"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, URIs: []*url.URL{identity}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	if principal(t, uri).Role() == Client {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	if change != nil {
		change(template)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return testCertificate{pair, certPEM, keyPEM}
}

func (ca testCA) pem() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw})
}
func writeTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}
func loadTestMaterial(t *testing.T, cert testCertificate, ca testCA, expected string) *Material {
	t.Helper()
	dir := t.TempDir()
	cp, kp, ap := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"), filepath.Join(dir, "ca.pem")
	writeTestFile(t, cp, cert.certPEM)
	writeTestFile(t, kp, cert.keyPEM)
	writeTestFile(t, ap, ca.pem())
	m, err := LoadMaterial(cp, kp, ap, principal(t, expected))
	if err != nil {
		t.Fatal(err)
	}
	return m
}
