package main

import (
	"errors"
	"flag"
	"net/url"

	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func workerSecurityFlags(fs *flag.FlagSet) *rpcauth.CoordinatorClientConfig {
	c := &rpcauth.CoordinatorClientConfig{}
	fs.StringVar(&c.CertFile, "tls-cert", "", "Worker certificate file (required)")
	fs.StringVar(&c.KeyFile, "tls-key", "", "Worker private key file (required)")
	fs.StringVar(&c.CAFile, "tls-ca", "", "trusted application CA bundle (required)")
	fs.StringVar(&c.ServerIdentity, "coordinator-identity", rpcauth.CoordinatorIdentity, "expected Coordinator URI SAN identity")
	fs.StringVar(&c.ServerName, "coordinator-server-name", "", "expected DNS/IP SAN (defaults to Coordinator address hostname/IP)")
	return c
}

func workerCoordinatorCredentials(workerID, address string, c rpcauth.CoordinatorClientConfig) (credentials.TransportCredentials, error) {
	transport, err := loadWorkerTransport(workerID, address, c)
	if err != nil {
		return nil, err
	}
	return transport.client, nil
}

type workerTransport struct {
	material    *rpcauth.Material
	coordinator rpcauth.Principal
	client      credentials.TransportCredentials
}

// Load the dual-use Worker identity once for inbound execution and outbound
// registration/heartbeats. All checks precede the Worker listener.
func loadWorkerTransport(workerID, address string, c rpcauth.CoordinatorClientConfig) (*workerTransport, error) {
	local, err := rpcauth.ParsePrincipal("spiffe://distributedzk.local/worker/" + url.PathEscape(workerID))
	if err != nil {
		return nil, errors.New("invalid Worker -id")
	}
	coordinator, err := rpcauth.ParsePrincipal(c.ServerIdentity)
	if err != nil || coordinator.Role() != rpcauth.Coordinator {
		return nil, errors.New("invalid -coordinator-identity")
	}
	if c.CertFile == "" || c.KeyFile == "" || c.CAFile == "" {
		return nil, errors.New("-tls-cert, -tls-key and -tls-ca are required")
	}
	material, err := rpcauth.LoadMaterial(c.CertFile, c.KeyFile, c.CAFile, local)
	if err != nil {
		return nil, err
	}
	client, err := c.CredentialsFromMaterial(address, material)
	if err != nil {
		return nil, err
	}
	return &workerTransport{material, coordinator, client}, nil
}

func (t *workerTransport) newServer(service *WorkerService) (*grpc.Server, error) {
	return rpcauth.NewWorkerServer(t.material, t.coordinator, service)
}

// Both startup registration and the long-lived heartbeat connection use these
// preloaded credentials. Clone isolates each connection's transport instance.
func newCoordinatorConnection(address string, transport credentials.TransportCredentials) (*grpc.ClientConn, error) {
	if transport == nil {
		return nil, errors.New("coordinator mTLS credentials required")
	}
	return grpc.NewClient(address,
		grpc.WithTransportCredentials(transport.Clone()),
		grpc.WithPerRPCCredentials(rpcauth.PeerValidityCredentials{}),
	)
}
