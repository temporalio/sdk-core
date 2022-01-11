package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	_, thisFile, _, _ := runtime.Caller(0)
	thisDir := filepath.Dir(thisFile)
	localProtoDir := filepath.Join(thisDir, "../../../../protos/local")
	const useGogo = true
	var commonArgs []string

	// Get the gogo protobuf path and change go packages for well known types
	binary := "go"
	if useGogo {
		binary = "gogoslick"
		b, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/gogo/protobuf").CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed getting module path for gogo protobuf: %w, output: %s", err, b)
		}
		commonArgs = append(
			commonArgs,
			"-I="+strings.TrimSpace(string(b)),
			"--gogoslick_opt=Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types",
			"--gogoslick_opt=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types",
			"--gogoslick_opt=Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types",
			"--gogoslick_opt=Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types",
			"--gogoslick_opt=Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types",
		)
	}

	// Key is proto, value is dir/package
	protos := map[string]string{
		"temporal/sdk/core/activity_result/activity_result.proto":         "corepb/activityresultpb",
		"temporal/sdk/core/activity_task/activity_task.proto":             "corepb/activitytaskpb",
		"temporal/sdk/core/bridge/bridge.proto":                           "corepb/bridgepb",
		"temporal/sdk/core/child_workflow/child_workflow.proto":           "corepb/childworkflowpb",
		"temporal/sdk/core/common/common.proto":                           "corepb/commonpb",
		"temporal/sdk/core/core_interface.proto":                          "corepb",
		"temporal/sdk/core/external_data/external_data.proto":             "corepb/externaldatapb",
		"temporal/sdk/core/workflow_activation/workflow_activation.proto": "corepb/workflowactivationpb",
		"temporal/sdk/core/workflow_commands/workflow_commands.proto":     "corepb/workflowcommandspb",
		"temporal/sdk/core/workflow_completion/workflow_completion.proto": "corepb/workflowcompletionpb",
	}
	// Create go package args
	for file, dir := range protos {
		// TODO(cretz): Does not change go_package name using gogo :-(
		commonArgs = append(commonArgs,
			"--"+binary+"_opt=M"+file+"="+"github.com/temporalio/sdk-core/bridge-ffi/example/goffi/"+dir)
	}

	// Generate protos
	for file, dir := range protos {
		// Create dir
		outDir := filepath.Join(thisDir, "..", dir)
		if err := os.MkdirAll(outDir, 0755); err != nil {
			return fmt.Errorf("failed creating dir %v: %w", outDir, err)
		}
		// Protoc args
		args := []string{
			"-I=" + filepath.Join(localProtoDir, file, ".."),
			"-I=" + localProtoDir,
			"-I=" + filepath.Join(thisDir, "../../../../protos/api_upstream"),
			"--" + binary + "_opt=paths=source_relative",
			"--" + binary + "_out=" + outDir,
		}
		args = append(args, commonArgs...)
		// args = append(args, filepath.Join(localProtoDir, file))
		args = append(args, filepath.Join(localProtoDir, file))
		log.Printf("Running protoc %v", strings.Join(args, " "))
		cmd := exec.Command("protoc", args...)
		cmd.Dir = thisDir
		cmd.Stdin = os.Stdin
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("protoc failed: %w", err)
		}
	}
	return nil
}
