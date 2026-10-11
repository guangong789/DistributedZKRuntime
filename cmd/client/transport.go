package main

import (
	"errors"
	"flag"

	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
	"google.golang.org/grpc/credentials"
)

type clientSecurityConfig struct {
	rpcauth.CoordinatorClientConfig
	Identity string
}

func clientSecurityFlags(fs *flag.FlagSet) *clientSecurityConfig {
	c := &clientSecurityConfig{}
	fs.StringVar(&c.CertFile, "tls-cert", "", "Client certificate file (required)")
	fs.StringVar(&c.KeyFile, "tls-key", "", "Client private key file (required)")
	fs.StringVar(&c.CAFile, "tls-ca", "", "trusted application CA bundle (required)")
	fs.StringVar(&c.Identity, "identity", "spiffe://distributedzk.local/client/client-1", "local Client URI SAN identity")
	fs.StringVar(&c.ServerIdentity, "coordinator-identity", rpcauth.CoordinatorIdentity, "expected Coordinator URI SAN identity")
	fs.StringVar(&c.ServerName, "coordinator-server-name", "", "expected DNS/IP SAN (defaults to Coordinator address hostname/IP)")
	return c
}

func (c clientSecurityConfig) credentials(address string) (credentials.TransportCredentials, error) {
	local, err := rpcauth.ParsePrincipal(c.Identity)
	if err != nil || local.Role() != rpcauth.Client {
		return nil, errors.New("invalid Client -identity")
	}
	return c.CoordinatorClientConfig.Credentials(address, local)
}
