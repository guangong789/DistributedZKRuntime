// Command dev-pki verifies bootstrap output without starting any service.
package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
)

func main() {
	dir := flag.String("dir", "", "bootstrap directory to verify (required; read-only)")
	flag.Parse()
	if *dir == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: go run ./scripts/dev-pki -dir DIRECTORY")
		os.Exit(1)
	}
	if err := verify(*dir); err != nil {
		fmt.Fprintln(os.Stderr, "dev-pki verification:", err)
		os.Exit(1)
	}
	fmt.Println("Go rpcauth verification passed: exact identities, SANs, EKUs, key pairs and TLS 1.3 configuration")
}

func certificate(path string) (*x509.Certificate, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return nil, errors.New("certificate must be a regular file with mode 0600")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, rest := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("expected exactly one PEM certificate")
	}
	return x509.ParseCertificate(block.Bytes)
}

func verify(dir string) error {
	caPath := filepath.Join(dir, "ca", "ca.crt")
	ca, err := certificate(caPath)
	if err != nil {
		return err
	}
	if !ca.IsCA || !ca.BasicConstraintsValid || ca.KeyUsage != x509.KeyUsageCertSign|x509.KeyUsageCRLSign || ca.NotAfter.Sub(ca.NotBefore) != 30*24*time.Hour {
		return errors.New("unexpected development CA constraints or lifetime")
	}
	if err := ca.CheckSignatureFrom(ca); err != nil {
		return err
	}
	publicKeys := make(map[string]bool)
	serials := make(map[string]bool)
	profiles := []struct {
		node, uri string
		usages    []x509.ExtKeyUsage
	}{
		{"ca", "", nil},
		{"coordinator", rpcauth.CoordinatorIdentity, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}},
		{"worker-1", "spiffe://distributedzk.local/worker/worker-1", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}},
		{"client-1", "spiffe://distributedzk.local/client/client-1", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}},
	}
	materials := make(map[string]*rpcauth.Material)
	principals := make(map[string]rpcauth.Principal)
	for _, profile := range profiles {
		nodeDir := filepath.Join(dir, profile.node)
		for _, path := range []string{dir, nodeDir} {
			info, err := os.Lstat(path)
			if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
				return errors.New("PKI directories must have mode 0700 and not be symlinks")
			}
		}
		certPath, keyPath := filepath.Join(nodeDir, "cert.pem"), filepath.Join(nodeDir, "key.pem")
		if profile.node == "ca" {
			certPath, keyPath = caPath, filepath.Join(nodeDir, "ca.key")
		}
		info, err := os.Lstat(keyPath)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			return errors.New("private keys must be regular files with mode 0600")
		}
		_, err = tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return errors.New("certificate/key pair mismatch or invalid encoding")
		}
		cert, err := certificate(certPath)
		if err != nil {
			return err
		}
		key, ok := cert.PublicKey.(*ecdsa.PublicKey)
		if !ok || key.Curve != elliptic.P256() || cert.SignatureAlgorithm != x509.ECDSAWithSHA256 {
			return errors.New("expected ECDSA P-256 with SHA-256")
		}
		public, err := x509.MarshalPKIXPublicKey(cert.PublicKey)
		if err != nil {
			return err
		}
		if publicKeys[string(public)] || serials[cert.SerialNumber.String()] || cert.SerialNumber.Sign() <= 0 {
			return errors.New("keys and positive serial numbers must be unique")
		}
		publicKeys[string(public)], serials[cert.SerialNumber.String()] = true, true
		if profile.node == "ca" {
			continue
		}
		if cert.IsCA || !cert.BasicConstraintsValid || cert.KeyUsage != x509.KeyUsageDigitalSignature || cert.NotAfter.Sub(cert.NotBefore) != 7*24*time.Hour || !cert.NotAfter.Before(ca.NotAfter) {
			return errors.New("unexpected leaf constraints or lifetime")
		}
		if len(cert.URIs) != 1 || cert.URIs[0].String() != profile.uri || len(cert.ExtKeyUsage) != len(profile.usages) || len(cert.UnknownExtKeyUsage) != 0 {
			return errors.New("unexpected leaf URI SAN or EKU")
		}
		for _, usage := range profile.usages {
			if !slices.Contains(cert.ExtKeyUsage, usage) {
				return errors.New("required role EKU missing")
			}
		}
		if profile.node == "client-1" {
			if len(cert.IPAddresses) != 0 || len(cert.DNSNames) != 0 {
				return errors.New("Client must not have server SANs")
			}
		} else if len(cert.IPAddresses) != 1 || !cert.IPAddresses[0].Equal(net.ParseIP("127.0.0.1")) || len(cert.DNSNames) != 1 || cert.DNSNames[0] != "localhost" {
			return errors.New("expected loopback IP and localhost DNS SANs")
		}
		principal, err := rpcauth.ParsePrincipal(profile.uri)
		if err != nil {
			return err
		}
		material, err := rpcauth.LoadMaterial(certPath, keyPath, caPath, principal)
		if err != nil {
			return err
		}
		materials[profile.node], principals[profile.node] = material, principal
	}
	policy, err := rpcauth.NewPolicy(principals["coordinator"], []rpcauth.Principal{principals["client-1"]}, []rpcauth.Principal{principals["worker-1"]})
	if err != nil {
		return err
	}
	for _, node := range []string{"coordinator", "worker-1"} {
		config, err := rpcauth.NewServerTLS(materials[node], policy)
		if err != nil || config.MinVersion != tls.VersionTLS13 || config.ClientAuth != tls.RequireAndVerifyClientCert {
			return errors.New("runtime mTLS server configuration rejected")
		}
	}
	for _, node := range []string{"client-1", "worker-1"} {
		if _, err := rpcauth.NewClientTLS(materials[node], "127.0.0.1", principals["coordinator"]); err != nil {
			return err
		}
	}
	_, err = materials["coordinator"].WorkerCredentials("worker-1", "127.0.0.1:50051")
	return err
}
