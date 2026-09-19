package main

import (
	"context"
	"fmt"
	"log"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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
		30 * time.Second,
	)
	defer cancel()

	resp, err := client.SubmitJob(
		ctx,
		&runtimepb.SubmitJobRequest{
			JobId:     500,
			TaskType:  "sleep",
			Payload:   "10s",
			TimeoutMs: 150000,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf(
		"job=%d status=%s output=%s error=%s\n",
		resp.JobId,
		resp.Status,
		resp.Output,
		resp.Error,
	)
}