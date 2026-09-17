package main

import (
	"fmt"
	"time"
)

func main() {
	executor := NewExecutor(2, 4)

	err := executor.Submit(Job{
		ID:      1,
		Timeout: 5 * time.Second,
		Task: SleepTask{
			Duration: 2 * time.Second,
		},
	})
	if err != nil {
		fmt.Println("submit failed:", err)
	}

	err = executor.Submit(Job{
		ID:      2,
		Timeout: 1 * time.Second,
		Task: HashTask{
			Input: "hello",
		},
	})
	if err != nil {
		fmt.Println("submit failed:", err)
	}

	time.Sleep(3 * time.Second)

	job, ok := executor.GetJob(1)
	if ok {
		fmt.Println("job status:", job.Status)
	}

	go executor.Shutdown()

	for result := range executor.Results() {
		fmt.Printf(
			"job=%d status=%d output=%s err=%v\n",
			result.JobID,
			result.Status,
			result.Output,
			result.Err,
		)
	}
}
