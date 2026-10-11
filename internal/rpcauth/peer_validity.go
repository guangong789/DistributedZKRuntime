package rpcauth

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// PeerValidityCredentials rechecks the verified server chain before a new RPC
// is sent on a reused connection. TLS already checked hostname and exact URI
// at handshake; this guard prevents a long-lived channel extending authority
// past certificate expiry. It adds no metadata and does not abort admitted RPCs.
type PeerValidityCredentials struct{}

func (PeerValidityCredentials) RequireTransportSecurity() bool { return true }

func (PeerValidityCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	request, ok := credentials.RequestInfoFromContext(ctx)
	if ok {
		if info, ok := request.AuthInfo.(credentials.TLSInfo); ok && info.State.HandshakeComplete {
			if _, err := verifiedPrincipal(info.State, time.Now()); err == nil {
				return nil, nil
			}
		}
	}
	return nil, status.Error(codes.Unauthenticated, "currently valid verified server identity required")
}
