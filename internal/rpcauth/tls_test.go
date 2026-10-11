package rpcauth

import (
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/url"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadMaterial(t *testing.T) {
	ca, other := newTestCA(t), newTestCA(t)
	valid := ca.issue(t, workerURI, nil)
	for _, tc := range []struct {
		name     string
		cert     testCertificate
		caPEM    []byte
		expected string
	}{
		{"valid worker", valid, ca.pem(), workerURI},
		{"untrusted CA", valid, other.pem(), workerURI},
		{"empty CA", valid, nil, workerURI},
		{"malformed CA", valid, []byte("invalid PEM"), workerURI},
		{"non CA trust anchor", valid, valid.certPEM, workerURI},
		{"trailing CA garbage", valid, append(ca.pem(), []byte("garbage")...), workerURI},
		{"wrong local ID", valid, ca.pem(), workerBURI},
		{"wrong local role", valid, ca.pem(), clientURI},
		{"expired", ca.issue(t, workerURI, func(c *x509.Certificate) { c.NotAfter = time.Now().Add(-time.Minute) }), ca.pem(), workerURI},
		{"future", ca.issue(t, workerURI, func(c *x509.Certificate) { c.NotBefore = time.Now().Add(time.Minute) }), ca.pem(), workerURI},
		{"client EKU only", ca.issue(t, workerURI, func(c *x509.Certificate) { c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth} }), ca.pem(), workerURI},
		{"server EKU only", ca.issue(t, workerURI, func(c *x509.Certificate) { c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth} }), ca.pem(), workerURI},
		{"any EKU", ca.issue(t, workerURI, func(c *x509.Certificate) { c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageAny} }), ca.pem(), workerURI},
		{"absent EKU", ca.issue(t, workerURI, func(c *x509.Certificate) { c.ExtKeyUsage = nil }), ca.pem(), workerURI},
		{"missing URI", ca.issue(t, workerURI, func(c *x509.Certificate) { c.URIs = nil }), ca.pem(), workerURI},
		{"multiple URI", ca.issue(t, workerURI, func(c *x509.Certificate) { c.URIs = append(c.URIs, c.URIs[0]) }), ca.pem(), workerURI},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cp, kp, ap := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"), filepath.Join(dir, "ca.pem")
			writeTestFile(t, cp, tc.cert.certPEM)
			writeTestFile(t, kp, tc.cert.keyPEM)
			writeTestFile(t, ap, tc.caPEM)
			got, err := LoadMaterial(cp, kp, ap, principal(t, tc.expected))
			if tc.name == "valid worker" {
				if err != nil || got == nil {
					t.Fatalf("valid load: %v", err)
				}
				return
			}
			if err == nil || got != nil {
				t.Fatal("invalid local credentials accepted")
			}
		})
	}
	t.Run("key mismatch and missing files", func(t *testing.T) {
		dir := t.TempDir()
		cp, kp, ap := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"), filepath.Join(dir, "ca.pem")
		writeTestFile(t, cp, valid.certPEM)
		writeTestFile(t, kp, other.issue(t, workerURI, nil).keyPEM)
		writeTestFile(t, ap, ca.pem())
		if m, err := LoadMaterial(cp, kp, ap, principal(t, workerURI)); err == nil || m != nil {
			t.Fatal("mismatched key accepted")
		}
		if m, err := LoadMaterial(cp+"missing", kp, ap, principal(t, workerURI)); err == nil || m != nil {
			t.Fatal("missing certificate accepted")
		}
		writeTestFile(t, kp, valid.keyPEM)
		if m, err := LoadMaterial(cp, kp, ap+"missing", principal(t, workerURI)); err == nil || m != nil {
			t.Fatal("missing trust bundle accepted")
		}
	})
}

func TestTLSConfiguration(t *testing.T) {
	ca := newTestCA(t)
	policy := testPolicy(t)
	server := loadTestMaterial(t, ca.issue(t, coordURI, nil), ca, coordURI)
	client := loadTestMaterial(t, ca.issue(t, clientURI, nil), ca, clientURI)
	sc, err := NewServerTLS(server, policy)
	if err != nil {
		t.Fatal(err)
	}
	cc, err := NewClientTLS(client, "127.0.0.1", principal(t, coordURI))
	if err != nil {
		t.Fatal(err)
	}
	if sc.MinVersion != tls.VersionTLS13 || cc.MinVersion != tls.VersionTLS13 || sc.ClientAuth != tls.RequireAndVerifyClientCert || cc.InsecureSkipVerify || sc.InsecureSkipVerify || len(cc.Certificates) != 1 {
		t.Fatal("insecure configuration")
	}
	for _, m := range []*Material{nil, {}, client} {
		if cfg, err := NewServerTLS(m, policy); err == nil || cfg != nil {
			t.Fatal("invalid server material accepted")
		}
	}
	if cfg, err := NewServerTLS(server, nil); err == nil || cfg != nil {
		t.Fatal("missing policy accepted")
	}
	for _, tc := range []struct {
		m        *Material
		name     string
		expected Principal
	}{
		{nil, "localhost", principal(t, coordURI)}, {client, "", principal(t, coordURI)}, {client, "localhost", Principal{}}, {client, "localhost", principal(t, clientURI)},
	} {
		if cfg, err := NewClientTLS(tc.m, tc.name, tc.expected); err == nil || cfg != nil {
			t.Fatal("invalid client config accepted")
		}
	}
}

// tlsRoundTrip performs actual TCP/TLS authentication in both directions and an
// application exchange. The exchange observes TLS 1.3 server-side alerts that
// can arrive after the client's own handshake has completed.
func tlsRoundTrip(t *testing.T, serverConfig, clientConfig *tls.Config) (error, error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	done := make(chan error, 1)
	go func() {
		raw, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		conn := tls.Server(raw, serverConfig)
		defer conn.Close()
		_ = conn.SetDeadline(deadline)
		if err := conn.HandshakeContext(ctx); err != nil {
			done <- err
			return
		}
		var b [1]byte
		if _, err := conn.Read(b[:]); err != nil {
			done <- err
			return
		}
		_, err = conn.Write(b[:])
		done <- err
	}()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn := tls.Client(raw, clientConfig)
	defer conn.Close()
	_ = conn.SetDeadline(deadline)
	clientErr := conn.HandshakeContext(ctx)
	if clientErr == nil {
		_, clientErr = conn.Write([]byte{1})
	}
	if clientErr == nil {
		var b [1]byte
		_, clientErr = conn.Read(b[:])
	}
	_ = conn.Close()
	select {
	case serverErr := <-done:
		return clientErr, serverErr
	case <-ctx.Done():
		t.Fatal("TLS server did not finish")
		return ctx.Err(), ctx.Err()
	}
}

func TestLocalhostMutualTLS(t *testing.T) {
	ca, untrusted := newTestCA(t), newTestCA(t)
	worker := ca.issue(t, workerURI, nil)
	coordinator := ca.issue(t, coordURI, nil)
	wm, cm := loadTestMaterial(t, worker, ca, workerURI), loadTestMaterial(t, coordinator, ca, coordURI)
	for _, tc := range []struct {
		name    string
		edit    func(*tls.Config, *tls.Config)
		success bool
	}{
		{"trusted IP", nil, true},
		{"trusted DNS", func(s, c *tls.Config) { c.ServerName = "localhost" }, true},
		{"wrong hostname correct URI", func(s, c *tls.Config) { c.ServerName = "wrong.example" }, false},
		{"correct hostname wrong Worker URI", func(s, c *tls.Config) { s.Certificates = []tls.Certificate{ca.issue(t, workerBURI, nil).pair} }, false},
		{"untrusted server", func(s, c *tls.Config) { s.Certificates = []tls.Certificate{untrusted.issue(t, workerURI, nil).pair} }, false},
		{"untrusted client", func(s, c *tls.Config) { forceClientCertificate(c, untrusted.issue(t, coordURI, nil).pair) }, false},
		{"missing client certificate", func(s, c *tls.Config) { c.Certificates = nil }, false},
		{"expired server", func(s, c *tls.Config) {
			s.Certificates = []tls.Certificate{ca.issue(t, workerURI, func(x *x509.Certificate) { x.NotAfter = time.Now().Add(-time.Minute) }).pair}
		}, false},
		{"future server", func(s, c *tls.Config) {
			s.Certificates = []tls.Certificate{ca.issue(t, workerURI, func(x *x509.Certificate) { x.NotBefore = time.Now().Add(time.Minute) }).pair}
		}, false},
		{"expired client", func(s, c *tls.Config) {
			forceClientCertificate(c, ca.issue(t, coordURI, func(x *x509.Certificate) { x.NotAfter = time.Now().Add(-time.Minute) }).pair)
		}, false},
		{"future client", func(s, c *tls.Config) {
			forceClientCertificate(c, ca.issue(t, coordURI, func(x *x509.Certificate) { x.NotBefore = time.Now().Add(time.Minute) }).pair)
		}, false},
		{"wrong server EKU", func(s, c *tls.Config) {
			s.Certificates = []tls.Certificate{ca.issue(t, workerURI, func(x *x509.Certificate) { x.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth} }).pair}
		}, false},
		{"wrong client EKU", func(s, c *tls.Config) {
			forceClientCertificate(c, ca.issue(t, coordURI, func(x *x509.Certificate) { x.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth} }).pair)
		}, false},
		{"client missing URI", func(s, c *tls.Config) {
			forceClientCertificate(c, ca.issue(t, coordURI, func(x *x509.Certificate) { x.URIs = nil }).pair)
		}, false},
		{"server missing URI", func(s, c *tls.Config) {
			s.Certificates = []tls.Certificate{ca.issue(t, workerURI, func(x *x509.Certificate) { x.URIs = nil }).pair}
		}, false},
		{"server ambiguous URI", func(s, c *tls.Config) {
			s.Certificates = []tls.Certificate{ca.issue(t, workerURI, func(x *x509.Certificate) { x.URIs = append(x.URIs, x.URIs[0]) }).pair}
		}, false},
		{"server wrong role", func(s, c *tls.Config) {
			s.Certificates = []tls.Certificate{ca.issue(t, workerURI, func(x *x509.Certificate) { u, _ := url.Parse(coordURI); x.URIs = []*url.URL{u} }).pair}
		}, false},
		{"unknown client identity", func(s, c *tls.Config) {
			forceClientCertificate(c, ca.issue(t, "spiffe://distributedzk.local/worker/unknown", nil).pair)
		}, false},
		{"TLS 1.2 rejected", func(s, c *tls.Config) { c.MinVersion = tls.VersionTLS12; c.MaxVersion = tls.VersionTLS12 }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, err := NewServerTLS(wm, testPolicy(t))
			if err != nil {
				t.Fatal(err)
			}
			cc, err := NewClientTLS(cm, "127.0.0.1", principal(t, workerURI))
			if err != nil {
				t.Fatal(err)
			}
			if tc.edit != nil {
				tc.edit(sc, cc)
			}
			clientErr, serverErr := tlsRoundTrip(t, sc, cc)
			if tc.success {
				if clientErr != nil || serverErr != nil {
					t.Fatalf("handshake/exchange failed: client=%v server=%v", clientErr, serverErr)
				}
			} else if clientErr == nil || serverErr == nil {
				t.Fatalf("invalid connection accepted: client=%v server=%v", clientErr, serverErr)
			}
		})
	}
}

func forceClientCertificate(config *tls.Config, cert tls.Certificate) {
	config.Certificates = nil
	config.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &cert, nil }
}

func TestIntermediateChainVerification(t *testing.T) {
	root := newTestCA(t)
	for _, restricted := range []bool{false, true} {
		t.Run(map[bool]string{false: "dual use chain", true: "client only intermediate"}[restricted], func(t *testing.T) {
			intermediate := root.issue(t, workerURI, func(c *x509.Certificate) {
				c.IsCA = true
				c.KeyUsage = x509.KeyUsageCertSign
				c.URIs = nil
				if restricted {
					c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
				}
			})
			key := intermediate.pair.PrivateKey.(*ecdsa.PrivateKey)
			issuer := testCA{intermediate.pair.Leaf, key}
			leaf := issuer.issue(t, workerURI, nil)
			dir := t.TempDir()
			cp, kp, ap := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"), filepath.Join(dir, "ca.pem")
			writeTestFile(t, kp, leaf.keyPEM)
			writeTestFile(t, ap, root.pem())
			writeTestFile(t, cp, leaf.certPEM)
			if _, err := LoadMaterial(cp, kp, ap, principal(t, workerURI)); err == nil {
				t.Fatal("missing intermediate accepted")
			}
			writeTestFile(t, cp, append(leaf.certPEM, intermediate.certPEM...))
			m, err := LoadMaterial(cp, kp, ap, principal(t, workerURI))
			if restricted {
				if err == nil || m != nil {
					t.Fatal("chain lacking ServerAuth accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			sc, err := NewServerTLS(m, testPolicy(t))
			if err != nil {
				t.Fatal(err)
			}
			client := loadTestMaterial(t, root.issue(t, coordURI, nil), root, coordURI)
			cc, err := NewClientTLS(client, "localhost", principal(t, workerURI))
			if err != nil {
				t.Fatal(err)
			}
			if ce, se := tlsRoundTrip(t, sc, cc); ce != nil || se != nil {
				t.Fatalf("intermediate chain rejected: client=%v server=%v", ce, se)
			}
		})
	}
}

func TestTLSClientDoesNotFallBackToPlaintext(t *testing.T) {
	ca := newTestCA(t)
	m := loadTestMaterial(t, ca.issue(t, coordURI, nil), ca, coordURI)
	cfg, err := NewClientTLS(m, "127.0.0.1", principal(t, workerURI))
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	received := make(chan byte, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			received <- 0
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(deadline)
		var b [1]byte
		if _, err := conn.Read(b[:]); err != nil {
			received <- 0
			return
		}
		// Deliberately respond as a plaintext endpoint, not as a TLS peer.
		_, _ = conn.Write([]byte("HTTP/1.1 400 Bad Request\r\n\r\n"))
		received <- b[0]
	}()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn := tls.Client(raw, cfg)
	defer conn.Close()
	if err := conn.HandshakeContext(ctx); err == nil {
		t.Fatal("plaintext endpoint accepted")
	}
	select {
	case first := <-received:
		if first != 22 {
			t.Fatal("client did not start with a TLS handshake")
		}
	case <-ctx.Done():
		t.Fatal("plaintext endpoint did not finish")
	}
}

func TestTLSResumptionRechecksIdentityAndAllowlist(t *testing.T) {
	for _, boundary := range []string{"server identity", "client allowlist"} {
		t.Run(boundary, func(t *testing.T) {
			ca := newTestCA(t)
			server := loadTestMaterial(t, ca.issue(t, coordURI, nil), ca, coordURI)
			client := loadTestMaterial(t, ca.issue(t, clientURI, nil), ca, clientURI)
			sc, err := NewServerTLS(server, testPolicy(t))
			if err != nil {
				t.Fatal(err)
			}
			cc, err := NewClientTLS(client, "localhost", principal(t, coordURI))
			if err != nil {
				t.Fatal(err)
			}
			// Production currently has no client session cache. Enable it here
			// to prove VerifyConnection also protects a genuinely resumed TLS1.3
			// session, not merely a fabricated ConnectionState.
			cc.ClientSessionCache = tls.NewLRUClientSessionCache(1)
			resumed := make(chan bool, 3)
			observe := sc.VerifyConnection
			sc.VerifyConnection = func(state tls.ConnectionState) error {
				resumed <- state.DidResume
				return observe(state)
			}
			for i := 0; i < 2; i++ {
				ce, se := tlsRoundTrip(t, sc, cc)
				if ce != nil || se != nil {
					t.Fatalf("valid exchange %d: client=%v server=%v", i, ce, se)
				}
				if got := <-resumed; got != (i == 1) {
					t.Fatalf("exchange %d resumed=%v", i, got)
				}
			}
			if boundary == "server identity" {
				// Retain the cache and SNI, but now require a different URI.
				wrong, err := NewClientTLS(client, "localhost", principal(t, workerURI))
				if err != nil {
					t.Fatal(err)
				}
				cc.VerifyConnection = func(state tls.ConnectionState) error {
					resumed <- state.DidResume
					return wrong.VerifyConnection(state)
				}
			} else {
				deny, err := NewPolicy(principal(t, coordURI), nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				changed, err := NewServerTLS(server, deny)
				if err != nil {
					t.Fatal(err)
				}
				// Preserve the original server's session ticket keys; only the
				// callback changes, sequentially after completed exchanges.
				sc.VerifyConnection = func(state tls.ConnectionState) error {
					resumed <- state.DidResume
					return changed.VerifyConnection(state)
				}
			}
			ce, se := tlsRoundTrip(t, sc, cc)
			if ce == nil || se == nil {
				t.Fatalf("resumed session bypassed %s: client=%v server=%v", boundary, ce, se)
			}
			if !<-resumed {
				t.Fatal("rejection did not exercise resumption")
			}
		})
	}
}
