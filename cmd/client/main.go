package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	conn, err := grpc.NewClient(
		"localhost:50050",
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := runtimepb.NewCoordinatorServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	payload, err := buildPreimagePayload("secret-001", demoPreimageDigest)
	if err != nil {
		log.Fatal(err)
	}

	resp, err := client.SubmitJob(
		ctx,
		&runtimepb.SubmitJobRequest{
			JobId:     2001,
			TaskType:  "zk_preimage_prove",
			Payload:   payload,
			TimeoutMs: 5000,
		},
	)
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
