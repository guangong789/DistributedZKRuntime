package rpcauth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"net/url"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestParsePrincipal(t *testing.T) {
	for _, tc := range []struct {
		uri  string
		role Role
		id   string
	}{
		{coordURI, Coordinator, "main"}, {workerURI, Worker, "worker-a"}, {clientURI, Client, "client-a"},
		{"spiffe://distributedzk.local/worker/Worker_A.01", Worker, "Worker_A.01"},
		{"spiffe://distributedzk.local/worker/team%2Fworker", Worker, "team/worker"},
		{"spiffe://distributedzk.local/worker/a%252Fb", Worker, "a%2Fb"},
		{"spiffe://distributedzk.local/worker/a%20b", Worker, "a b"},
		{"spiffe://distributedzk.local/worker/%E5%B7%A5%E4%BD%9C", Worker, "工作"},
		{"spiffe://distributedzk.local/worker/..", Worker, ".."},
	} {
		t.Run(tc.uri, func(t *testing.T) {
			p, err := ParsePrincipal(tc.uri)
			if err != nil || p.Role() != tc.role || p.ID() != tc.id || p.URI() != tc.uri {
				t.Fatalf("incorrect principal: %v", err)
			}
			wantWorker := ""
			if tc.role == Worker {
				wantWorker = tc.id
			}
			if p.WorkerID() != wantWorker {
				t.Fatal("incorrect WorkerID")
			}
		})
	}
	for _, uri := range []string{
		"", "https://distributedzk.local/worker/a", "SPIFFE://distributedzk.local/worker/a", "spiffe://other/worker/a",
		"spiffe://DISTRIBUTEDZK.local/worker/a", "spiffe://distributedzk.local:443/worker/a",
		"spiffe://user@distributedzk.local/worker/a", "spiffe://distributedzk.local/worker/a?q=x",
		"spiffe://distributedzk.local/worker/a?", "spiffe://distributedzk.local/worker/a#", "spiffe://distributedzk.local/worker/a#x",
		"spiffe://distributedzk.local/worker/", "spiffe://distributedzk.local/worker/a/b", "spiffe://distributedzk.local/worker/a/",
		"spiffe://distributedzk.local/worker", "spiffe://distributedzk.local/admin/a", "spiffe://distributedzk.local/Worker/a",
		"spiffe://distributedzk.local/coordinator/other", "spiffe://distributedzk.local/worker/a%", "spiffe://distributedzk.local/worker/%FF",
		"spiffe://distributedzk.local/worker/%61", "spiffe://distributedzk.local/worker/a%2fb", "spiffe://distributedzk.local/worker/a b",
	} {
		t.Run("reject/"+uri, func(t *testing.T) {
			if _, err := ParsePrincipal(uri); err == nil {
				t.Fatal("accepted invalid identity")
			}
		})
	}
}

func TestCertificatePrincipalRejectsAmbiguityAndNormalization(t *testing.T) {
	ca := newTestCA(t)
	for _, tc := range []struct {
		name   string
		modify func(*x509.Certificate)
	}{
		{"missing URI including CN fallback", func(c *x509.Certificate) { c.URIs = nil; c.Subject.CommonName = workerURI }},
		{"duplicate URI", func(c *x509.Certificate) { c.URIs = append(c.URIs, c.URIs[0]) }},
		{"different URI", func(c *x509.Certificate) { u, _ := url.Parse(clientURI); c.URIs = append(c.URIs, u) }},
		{"uppercase raw scheme", func(c *x509.Certificate) { setRawSAN(t, c, "SPIFFE://distributedzk.local/worker/worker-a") }},
		{"empty raw fragment", func(c *x509.Certificate) { setRawSAN(t, c, workerURI+"#") }},
		{"escaped ID alias", func(c *x509.Certificate) { setRawSAN(t, c, "spiffe://distributedzk.local/worker/%77orker-a") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cert := ca.issue(t, workerURI, tc.modify)
			if _, err := certificatePrincipal(cert.pair.Leaf); err == nil {
				t.Fatal("accepted ambiguous/noncanonical certificate identity")
			}
		})
	}
}

func setRawSAN(t *testing.T, c *x509.Certificate, uri string) {
	t.Helper()
	value, err := asn1.Marshal([]asn1.RawValue{{Class: asn1.ClassContextSpecific, Tag: 6, Bytes: []byte(uri)}})
	if err != nil {
		t.Fatal(err)
	}
	c.ExtraExtensions = []pkix.Extension{{Id: asn1.ObjectIdentifier{2, 5, 29, 17}, Value: value}}
}

func TestAuthenticatePeer(t *testing.T) {
	ca := newTestCA(t)
	cert := ca.issue(t, workerURI, nil)
	verified := tls.ConnectionState{Version: tls.VersionTLS13, HandshakeComplete: true, VerifiedChains: [][]*x509.Certificate{{cert.pair.Leaf, ca.cert}}}
	good := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: verified}})
	good = metadata.NewIncomingContext(good, metadata.Pairs("role", "coordinator", "worker-id", "worker-b"))
	p, err := AuthenticatePeer(good)
	if err != nil || p != principal(t, workerURI) {
		t.Fatalf("verified identity: %v", err)
	}
	unverified := verified
	unverified.VerifiedChains = nil
	unverified.PeerCertificates = []*x509.Certificate{cert.pair.Leaf}
	oldVersion := verified
	oldVersion.Version = tls.VersionTLS12
	incomplete := verified
	incomplete.HandshakeComplete = false
	for _, ctx := range []context.Context{
		context.Background(),
		metadata.NewIncomingContext(context.Background(), metadata.Pairs("identity", workerURI)),
		peer.NewContext(context.Background(), &peer.Peer{}),
		peer.NewContext(context.Background(), &peer.Peer{AuthInfo: insecureInfo(t)}),
		peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: unverified}}),
		peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: oldVersion}}),
		peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: incomplete}}),
	} {
		if _, err := AuthenticatePeer(ctx); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("error = %v", err)
		}
	}
	// Deterministically model an established connection after certificate expiry.
	if _, err := verifiedPrincipal(verified, cert.pair.Leaf.NotAfter.Add(time.Second)); err == nil {
		t.Fatal("expired established peer accepted")
	}
	if _, err := verifiedPrincipal(verified, cert.pair.Leaf.NotBefore.Add(-time.Second)); err == nil {
		t.Fatal("future peer accepted")
	}
}

func insecureInfo(t *testing.T) credentials.AuthInfo {
	t.Helper()
	// The insecure transport handshake does not read/write the connection.
	_, info, err := insecure.NewCredentials().ServerHandshake(nil)
	if err != nil {
		t.Fatal(err)
	}
	return info
}
