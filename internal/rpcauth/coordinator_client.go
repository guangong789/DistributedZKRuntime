package rpcauth

import (
	"errors"
	"fmt"
	"net"

	"google.golang.org/grpc/credentials"
)

const CoordinatorIdentity = "spiffe://distributedzk.local/coordinator/main"

// CoordinatorClientConfig is shared by Client submissions and Worker
// registration/heartbeats. Load credentials once; there is no insecure mode.
type CoordinatorClientConfig struct {
	CertFile       string
	KeyFile        string
	CAFile         string
	ServerIdentity string
	// Empty means verify the hostname/IP in the configured host:port address.
	ServerName string
}

func (c CoordinatorClientConfig) Credentials(address string, local Principal) (credentials.TransportCredentials, error) {
	if local.Role() != Client && local.Role() != Worker {
		return nil, errors.New("Client or Worker local identity required")
	}
	if c.CertFile == "" || c.KeyFile == "" || c.CAFile == "" {
		return nil, errors.New("-tls-cert, -tls-key and -tls-ca are required")
	}
	material, err := LoadMaterial(c.CertFile, c.KeyFile, c.CAFile, local)
	if err != nil {
		return nil, fmt.Errorf("coordinator client credentials: %w", err)
	}
	return c.CredentialsFromMaterial(address, material)
}

// CredentialsFromMaterial lets a dual-use Worker share one validated key pair
// and trust snapshot across server and client connections without reloading.
func (c CoordinatorClientConfig) CredentialsFromMaterial(address string, material *Material) (credentials.TransportCredentials, error) {
	if !material.valid() || (material.principal.Role() != Client && material.principal.Role() != Worker) {
		return nil, errors.New("Client or Worker local identity required")
	}
	expected, err := ParsePrincipal(c.ServerIdentity)
	if err != nil || expected.Role() != Coordinator {
		return nil, errors.New("invalid -coordinator-identity")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || port == "" {
		return nil, errors.New("-coordinator must be a host:port address")
	}
	name := c.ServerName
	if name == "" {
		name = host
	}
	config, err := NewClientTLS(material, name, expected)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(config), nil
}
