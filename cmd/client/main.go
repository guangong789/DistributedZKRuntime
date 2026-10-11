package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
)

const demoPreimageDigest = "9859286970797740035380527431348382675909558438535884267813507963157263542611"

func buildPreimagePayload(witnessRef, digest string) (string, error) {
	data, err := json.Marshal(struct {
		WitnessRef string `json:"witness_ref"`
		Digest     string `json:"digest"`
	}{
		WitnessRef: witnessRef,
		Digest:     digest,
	})
	return string(data), err
}

func proofByteLength(output string) int {
	proof, err := base64.StdEncoding.DecodeString(output)
	if err != nil {
		return 0
	}
	return len(proof)
}

func main() {
	coordinatorAddr := flag.String(
		"coordinator",
		"localhost:50050",
		"coordinator address",
	)

	execution := clientExecutionFlags(flag.CommandLine)
	security := clientSecurityFlags(flag.CommandLine)
	flag.Parse()
	if err := execution.validate(); err != nil {
		log.Fatalf("client execution configuration: %v", err)
	}
	transportCredentials, err := security.credentials(*coordinatorAddr)
	if err != nil {
		log.Fatalf("client mTLS configuration: %v", err)
	}

	conn, err := grpc.NewClient(
		*coordinatorAddr,
		grpc.WithTransportCredentials(transportCredentials),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := runtimepb.NewCoordinatorServiceClient(conn)

	resp, err := submitDemo(context.Background(), client, *execution)
	if err != nil {
		log.Fatal(err)
	}

	proofLength := 0
	if resp.Status == "Succeeded" {
		proofLength = proofByteLength(resp.Output)
	}

	fmt.Printf(
		"job=%d status=%s attempt=%d proof_bytes=%d error=%s\n",
		resp.JobId,
		resp.Status,
		resp.AttemptId,
		proofLength,
		resp.Error,
	)
}
