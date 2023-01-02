[![Build status](https://badge.buildkite.com/c23f47f4a827f04daece909963bd3a248496f0cdbabfbecee4.svg?branch=master)](https://buildkite.com/temporal/core-sdk?branch=master)

Core SDK that can be used as a base for other Temporal SDKs. It is currently used as the base of:

- [TypeScript SDK](https://github.com/temporalio/sdk-typescript/)
- [Python SDK](https://github.com/temporalio/sdk-python/)

For the reasoning behind the Core SDK, see: 

- [Why Rust powers Temporal’s new Core SDK](https://temporal.io/blog/why-rust-powers-core-sdk).

# Getting started

See the [Architecture](ARCHITECTURE.md) doc for some high-level information.

## Dependencies
* Protobuf compiler

# Development

This repo is composed of multiple crates:
* temporal-sdk-core-protos `./sdk-core-protos` - Holds the generated proto code and extensions
* temporal-client `./client` - Defines client(s) for interacting with the Temporal gRPC service
* temporal-sdk-core-api `./core-api` - Defines the API surface exposed by Core
* temporal-sdk-core `./core` - The Core implementation
* temporal-sdk `./sdk` - A (currently prototype) Rust SDK built on top of Core. Used for testing.
* rustfsm `./fsm` - Implements a procedural macro used by core for defining state machines
    (contains subcrates). It is temporal agnostic.

Visualized (dev dependencies are in blue):

![Crate dependency graph](./etc/deps.svg)


All the following commands are enforced for each pull request:

## Building and testing

You can buld and test the project using cargo:
`cargo build`
`cargo test`

Run integ tests with `cargo integ-test`. You will need to already be running the server:
`docker-compose -f .buildkite/docker/docker-compose.yaml up`

## Formatting
To format all code run:
`cargo fmt --all`

## Linting
We are using [clippy](https://github.com/rust-lang/rust-clippy) for linting.
You can run it using:
`cargo clippy --all -- -D warnings`

## Debugging
The crate uses [tracing](https://github.com/tokio-rs/tracing) to help with debugging. To enable
it for a test, insert the below snippet at the start of the test. By default, tracing data is output
to stdout in a (reasonably) pretty manner.

```rust
crate::telemetry::test_telem_console();
```

The passed in options to initialization can be customized to export to an OTel collector, etc.

To run integ tests with OTel collection on, you can use `integ-with-otel.sh`. You will want to make
sure you are running the collector via docker, which can be done like so:

`docker-compose -f .buildkite/docker/docker-compose.yaml -f .buildkite/docker/docker-compose-telem.yaml up`

If you are working on a language SDK, you are expected to initialize tracing early in your `main`
equivalent.

## Proto files

This repo uses a subtree for upstream protobuf files. The path `protos/api_upstream` is a
subtree. To update it, use:

`git pull --squash -s subtree ssh://git@github.com/temporalio/api.git master --allow-unrelated-histories`

Do not question why this git command is the way it is. It is not our place to interpret git's ways.

The java testserver protos are also pulled from the sdk-java repo, but since we only need a
subdirectory of that repo, we just copy the files with read-tree:
```bash
# add sdk-java as a remote if you have not already
git remote add -f -t master --no-tags testsrv-protos git@github.com:temporalio/sdk-java.git
# delete existing protos
git rm -rf protos/testsrv_upstream
# pull from upstream & commit
git read-tree --prefix protos/testsrv_upstream -u testsrv-protos/master:temporal-test-server/src/main/proto
git commit
```

## Fetching Histories
Tests which would like to replay stored histories rely on that history being made available in
binary format. You can fetch histories in that format like so (from a local docker server):

`cargo run --bin histfetch {workflow_id} [{run_id}]`

You can change the `TEMPORAL_SERVICE_ADDRESS` env var to fetch from a different address.

## Style Guidelines

### Error handling
Any error which is returned from a public interface should be well-typed, and we use 
[thiserror](https://github.com/dtolnay/thiserror) for that purpose.

Errors returned from things only used in testing are free to use 
[anyhow](https://github.com/dtolnay/anyhow) for less verbosity.

