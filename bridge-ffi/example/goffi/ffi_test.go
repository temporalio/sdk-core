package goffi_test

import (
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/temporalio/sdk-core/bridge-ffi/example/goffi"
	bridgepb "github.com/temporalio/sdk-core/bridge-ffi/example/goffi/corepb/bridgepb"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

func Example() {
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

	// TODO(cretz): the rest of this

	// Output:
}
