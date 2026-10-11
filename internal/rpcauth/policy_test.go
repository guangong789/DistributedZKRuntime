package rpcauth

import (
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestAuthorizeRPC(t *testing.T) {
	policy := testPolicy(t)
	roles := []Principal{principal(t, coordURI), principal(t, workerURI), principal(t, clientURI), principal(t, "spiffe://distributedzk.local/client/unknown"), {}}
	for _, tc := range []struct {
		method  string
		req     any
		allowed Role
	}{
		{runtimepb.CoordinatorService_SubmitJob_FullMethodName, &runtimepb.SubmitJobRequest{}, Client},
		{runtimepb.CoordinatorService_RegisterWorker_FullMethodName, &runtimepb.RegisterWorkerRequest{WorkerId: "worker-a"}, Worker},
		{runtimepb.CoordinatorService_Heartbeat_FullMethodName, &runtimepb.HeartbeatRequest{WorkerId: "worker-a"}, Worker},
		{runtimepb.WorkerService_ExecuteJob_FullMethodName, &runtimepb.ExecuteJobRequest{}, Coordinator},
		{"/runtime.CoordinatorService/Unknown", &runtimepb.SubmitJobRequest{}, ""},
		{"/runtime.CoordinatorService/submitjob", &runtimepb.SubmitJobRequest{}, ""},
		{runtimepb.CoordinatorService_RegisterWorker_FullMethodName, &runtimepb.RegisterWorkerRequest{WorkerId: "worker-b"}, ""},
		{runtimepb.CoordinatorService_Heartbeat_FullMethodName, &runtimepb.HeartbeatRequest{WorkerId: "worker-b"}, ""},
		{runtimepb.CoordinatorService_RegisterWorker_FullMethodName, &runtimepb.HeartbeatRequest{WorkerId: "worker-a"}, ""},
		{runtimepb.CoordinatorService_RegisterWorker_FullMethodName, (*runtimepb.RegisterWorkerRequest)(nil), ""},
		{runtimepb.CoordinatorService_SubmitJob_FullMethodName, nil, ""},
	} {
		for _, p := range roles {
			err := policy.AuthorizeRPC(p, tc.method, tc.req)
			allowed := p.valid() && p.ID() != "unknown" && p.Role() == tc.allowed
			if allowed && err != nil {
				t.Fatalf("%s %s denied: %v", p.Role(), tc.method, err)
			}
			if !allowed && status.Code(err) != codes.PermissionDenied {
				t.Fatalf("%s %s incorrectly permitted: %v", p.Role(), tc.method, err)
			}
		}
	}
	// Independent immutable fields prevent a caller-controlled role from
	// relabeling a configured client identity or manufacturing WorkerID authority.
	forged := principal(t, clientURI)
	forged.role = Worker
	if err := policy.AuthorizeRPC(forged, runtimepb.CoordinatorService_RegisterWorker_FullMethodName, &runtimepb.RegisterWorkerRequest{WorkerId: forged.ID()}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("forged role allowed")
	}
	var empty *Policy
	if err := empty.AuthorizeRPC(principal(t, clientURI), runtimepb.CoordinatorService_SubmitJob_FullMethodName, &runtimepb.SubmitJobRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("nil policy allowed")
	}
}

func TestPolicyConfigurationAndExactWorkerID(t *testing.T) {
	coordinator := principal(t, coordURI)
	for _, tc := range []struct {
		coordinator      Principal
		clients, workers []Principal
	}{
		{Principal{}, nil, nil}, {principal(t, clientURI), nil, nil},
		{coordinator, []Principal{principal(t, workerURI)}, nil},
		{coordinator, nil, []Principal{principal(t, clientURI)}},
	} {
		if _, err := NewPolicy(tc.coordinator, tc.clients, tc.workers); err == nil {
			t.Fatal("invalid policy accepted")
		}
	}
	w := principal(t, "spiffe://distributedzk.local/worker/team%2FWorker_A")
	workers := []Principal{w}
	policy, err := NewPolicy(coordinator, nil, workers)
	if err != nil {
		t.Fatal(err)
	}
	workers[0] = principal(t, workerBURI)
	for _, id := range []string{"team/Worker_A", "team%2FWorker_A", "team/worker_a"} {
		err := policy.AuthorizeRPC(w, runtimepb.CoordinatorService_Heartbeat_FullMethodName, &runtimepb.HeartbeatRequest{WorkerId: id})
		if (err == nil) != (id == "team/Worker_A") {
			t.Fatal("WorkerID binding changed bytes or allowlist retained mutable input")
		}
	}
}
