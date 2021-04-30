[![Build status](https://badge.buildkite.com/c23f47f4a827f04daece909963bd3a248496f0cdbabfbecee4.svg?branch=master)](https://buildkite.com/temporal/core-sdk?branch=master)

Core SDK that can be used as a base for all other Temporal SDKs.

# Getting started

See the [Architecture](ARCHITECTURE.md) doc for some high-level information.

This repo uses a submodule for upstream protobuf files. The path `protos/api_upstream` is a 
submodule -- when checking out the repo for the first time make sure you've run 
`git submodule update --init --recursive`. TODO: Makefile.

## Dependencies
* Protobuf compiler

# Development

All of the following commands are enforced for each pull request.

## Building and testing

You can buld and test the project using cargo:
`cargo build`
`cargo test`

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
to stdout in a (reasonably) pretty manner, and to a Jaeger instance if one exists.

```rust
core_tracing::tracing_init();
let s = info_span!("Test start");
let _enter = s.enter();
```

To run the Jaeger instance:
`docker run --rm -p6831:6831/udp -p6832:6832/udp -p16686:16686 jaegertracing/all-in-one:latest`
Jaeger collection is off by default, you must set `TEMPORAL_ENABLE_OPENTELEMENTRY=true` in the
environment to enable it.

To show logs in the console, set the `RUST_LOG` environment variable to `temporal_sdk_core=DEBUG`
or whatever level you desire. The env var is parsed according to tracing's 
[EnvFilter](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/struct.EnvFilter.html)
rules.

If you are working on a language SDK, you are expected to initialize tracing early in your `main`
equivalent.

## Style Guidelines

### Error handling
Any error which is returned from a public interface should be well-typed, and we use 
[thiserror](https://github.com/dtolnay/thiserror) for that purpose.

Errors returned from things only used in testing are free to use 
[anyhow](https://github.com/dtolnay/anyhow) for less verbosity.

