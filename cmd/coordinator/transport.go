package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"

	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type coordinatorSecurityConfig struct {
	CertFile, KeyFile, CAFile, Identity string
	Clients, Workers                    []string
}

func coordinatorSecurityFlags(fs *flag.FlagSet) *coordinatorSecurityConfig {
	c := &coordinatorSecurityConfig{}
	fs.StringVar(&c.CertFile, "tls-cert", "", "coordinator certificate file (required)")
	fs.StringVar(&c.KeyFile, "tls-key", "", "coordinator private key file (required)")
	fs.StringVar(&c.CAFile, "tls-ca", "", "trusted application CA bundle (required)")
	fs.StringVar(&c.Identity, "identity", rpcauth.CoordinatorIdentity, "local Coordinator URI SAN identity")
	fs.Func("allow-client", "allowed Client URI identity (repeatable; empty list denies all Clients)", func(value string) error {
		c.Clients = append(c.Clients, value)
		return nil
	})
	fs.Func("allow-worker", "allowed Worker URI identity (repeatable; empty list denies all Workers)", func(value string) error {
		c.Workers = append(c.Workers, value)
		return nil
	})
	return c
}

// Load before opening the store/listeners. This keeps startup fail-closed
// without making business methods depend on a transport or global policy.
func (c coordinatorSecurityConfig) load() (*coordinatorTransport, error) {
	local, err := rpcauth.ParsePrincipal(c.Identity)
	if err != nil || local.Role() != rpcauth.Coordinator {
		return nil, errors.New("invalid Coordinator -identity")
	}
	clients, err := allowedPrincipals(c.Clients, rpcauth.Client)
	if err != nil {
		return nil, fmt.Errorf("-allow-client: %w", err)
	}
	workers, err := allowedPrincipals(c.Workers, rpcauth.Worker)
	if err != nil {
		return nil, fmt.Errorf("-allow-worker: %w", err)
	}
	policy, err := rpcauth.NewPolicy(local, clients, workers)
	if err != nil {
		return nil, err
	}
	if c.CertFile == "" || c.KeyFile == "" || c.CAFile == "" {
		return nil, errors.New("-tls-cert, -tls-key and -tls-ca are required")
	}
	material, err := rpcauth.LoadMaterial(c.CertFile, c.KeyFile, c.CAFile, local)
	if err != nil {
		return nil, err
	}
	config, err := rpcauth.NewServerTLS(material, policy)
	if err != nil {
		return nil, err
	}
	return &coordinatorTransport{tls: config, policy: policy, material: material}, nil
}

func allowedPrincipals(identities []string, role rpcauth.Role) ([]rpcauth.Principal, error) {
	principals := make([]rpcauth.Principal, 0, len(identities))
	seen := make(map[rpcauth.Principal]bool, len(identities))
	for _, identity := range identities {
		p, err := rpcauth.ParsePrincipal(identity)
		if err != nil || p.Role() != role || seen[p] {
			return nil, errors.New("entries must be distinct, canonical identities with the correct role")
		}
		seen[p] = true
		principals = append(principals, p)
	}
	return principals, nil
}

type coordinatorTransport struct {
	tls      *tls.Config
	policy   *rpcauth.Policy
	material *rpcauth.Material
}

func (t *coordinatorTransport) workerCredentials(worker WorkerInfo) (credentials.TransportCredentials, error) {
	return t.material.WorkerCredentials(worker.ID, worker.Address)
}

func (t *coordinatorTransport) newServer(service *CoordinatorServer) *grpc.Server {
	if service.lifecycle == nil {
		service.lifecycle = newCoordinatorLifecycle()
	}
	server := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(t.tls)),
		grpc.ChainUnaryInterceptor(t.policy.UnaryInterceptor(), service.lifecycle.unaryInterceptor(), validateWorkerRegistration),
		grpc.UnknownServiceHandler(t.policy.UnknownServiceHandler()),
	)
	runtimepb.RegisterCoordinatorServiceServer(server, service)
	return server
}

// URI/WorkerID authorization runs first. Endpoint validation and SAN binding
// then run before registry publication, metrics or recovery notifications.
func validateWorkerRegistration(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if info.FullMethod == runtimepb.CoordinatorService_RegisterWorker_FullMethodName {
		registration := req.(*runtimepb.RegisterWorkerRequest)
		host, err := rpcauth.ParseTCPEndpoint(registration.Address)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid Worker TCP endpoint")
		}
		if err := rpcauth.VerifyPeerHostname(ctx, host); err != nil {
			return nil, err
		}
	}
	return handler(ctx, req)
}
