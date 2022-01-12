package goffi_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/temporalio/sdk-core/bridge-ffi/example/goffi"
	bridgepb "github.com/temporalio/sdk-core/bridge-ffi/example/goffi/corepb/bridgepb"
	commonpb "github.com/temporalio/sdk-core/bridge-ffi/example/goffi/corepb/commonpb"
	workflowactivationpb "github.com/temporalio/sdk-core/bridge-ffi/example/goffi/corepb/workflowactivationpb"
	workflowcommandspb "github.com/temporalio/sdk-core/bridge-ffi/example/goffi/corepb/workflowcommandspb"
	workflowcompletionpb "github.com/temporalio/sdk-core/bridge-ffi/example/goffi/corepb/workflowcompletionpb"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	log := log.New(os.Stdout, "", 0)

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

	// Start a poller to handle workflow activations
	go func() {
		req := &bridgepb.PollWorkflowActivationRequest{TaskQueue: taskQueue}
		for {
			// Get activation and check if shutdown
			act, err := core.PollWorkflowActivation(req)
			if goffi.IsPollErrShutdown(err) {
				log.Printf("Stopping poller due to shutdown")
				return
			} else if err != nil {
				log.Panicf("Error: %v", err)
			}

			// Handle each job
			for _, job := range act.Jobs {
				var commands []*workflowcommandspb.WorkflowCommand
				switch job := job.Variant.(type) {
				// This example we'll just respond to start with completion
				case *workflowactivationpb.WorkflowActivationJob_StartWorkflow:
					// Extract param
					var param string
					err := converter.GetDefaultDataConverter().FromPayload(&common.Payload{
						Metadata: job.StartWorkflow.Arguments[0].Metadata,
						Data:     job.StartWorkflow.Arguments[0].Data,
					}, &param)
					if err != nil {
						log.Panic(err)
					}
					log.Printf("Got start workflow activation request with argument %q", param)

					// Mark complete with an uppercased result
					res, err := converter.GetDefaultDataConverter().ToPayload(strings.ToUpper(param))
					if err != nil {
						log.Panic(err)
					}
					commands = append(commands, &workflowcommandspb.WorkflowCommand{
						Variant: &workflowcommandspb.WorkflowCommand_CompleteWorkflowExecution{
							CompleteWorkflowExecution: &workflowcommandspb.CompleteWorkflowExecution{
								Result: &commonpb.Payload{Metadata: res.Metadata, Data: res.Data},
							},
						},
					})

				case *workflowactivationpb.WorkflowActivationJob_RemoveFromCache:
					// Ignore

				default:
					log.Panicf("Unexpected job type: %T", job)
				}

				// Send successful activation
				err := core.CompleteWorkflowActivation(&bridgepb.CompleteWorkflowActivationRequest{
					Completion: &workflowcompletionpb.WorkflowActivationCompletion{
						TaskQueue: taskQueue,
						RunId:     act.RunId,
						Status: &workflowcompletionpb.WorkflowActivationCompletion_Successful{
							Successful: &workflowcompletionpb.Success{
								Commands: commands,
							},
						},
					},
				})
				if err != nil {
					log.Panic(err)
				}
			}
		}
	}()

	// Start a workflow
	sdkClient, err := client.NewClient(client.Options{Logger: nopLogger{}})
	if err != nil {
		log.Panic(err)
	}
	defer sdkClient.Close()
	run, err := sdkClient.ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{ID: "some-id-" + uuid.NewString(), TaskQueue: taskQueue},
		"workflow-to-uppercase",
		"foo bar",
	)
	if err != nil {
		log.Panic(err)
	}

	// Wait for completion and check the value
	var resp string
	if err := run.Get(ctx, &resp); err != nil {
		log.Panic(err)
	}

	log.Printf("Workflow completed with %q", resp)

	// Output:
	// Got start workflow activation request with argument "foo bar"
	// Workflow completed with "FOO BAR"
	// Stopping poller due to shutdown
}

type nopLogger struct{}

func (nopLogger) Debug(string, ...interface{}) {}
func (nopLogger) Info(string, ...interface{})  {}
func (nopLogger) Warn(string, ...interface{})  {}
func (nopLogger) Error(string, ...interface{}) {}
