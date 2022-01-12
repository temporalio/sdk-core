package goffi_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/temporalio/sdk-core/bridge-ffi/example/goffi"
	bridgepb "github.com/temporalio/sdk-core/bridge-ffi/example/goffi/corepb/bridgepb"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

func Example() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create runtime
	rt := goffi.NewRuntime()
	defer rt.Close()

	// Create core
	core, err := goffi.NewCore(rt, &bridgepb.InitRequest{
		GatewayOptions: &bridgepb.InitRequest_GatewayOptions{
			TargetUrl:      "http://" + client.DefaultHostPort,
			Namespace:      client.DefaultNamespace,
			ClientName:     "temporal-go",
			ClientVersion:  temporal.SDKVersion,
			WorkerBinaryId: fmt.Sprintf("test-binary-%v", uuid.NewString()),
		},
	})
	if err != nil {
		log.Panic(err)
	}
	defer core.Shutdown()

	// Start worker on our task queue
	taskQueue := "my-task-queue-" + uuid.NewString()
	err = core.RegisterWorker(&bridgepb.RegisterWorkerRequest{TaskQueue: taskQueue})
	if err != nil {
		log.Panic(err)
	}

	// Start a poller
	go func() {
		req := &bridgepb.PollWorkflowActivationRequest{TaskQueue: taskQueue}
		for {
			act, err := core.PollWorkflowActivation(req)
			if err != nil {
				log.Printf("Error: %v", err)
				return
			}
			log.Printf("Got activation: %v", act)
		}
	}()

	// Start a workflow
	sdkClient, err := client.NewClient(client.Options{})
	if err != nil {
		log.Panic(err)
	}
	defer sdkClient.Close()
	_, err = sdkClient.ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{ID: "some-id-123", TaskQueue: taskQueue},
		"my-test-workflow",
		"some string argument2",
	)
	if err != nil {
		log.Panic(err)
	}

	// TODO(cretz): the rest of this
	log.Printf("Workflow started")
	time.Sleep(4 * time.Second)
	log.Printf("TODO")

	// Output:
}
