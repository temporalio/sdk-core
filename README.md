[![Build status](https://badge.buildkite.com/c23f47f4a827f04daece909963bd3a248496f0cdbabfbecee4.svg)](https://buildkite.com/temporal/core-sdk?branch=master)

hello

Core SDK that can be used as a base for all other Temporal SDKs.

# Getting started
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
`cargo format --all`

## Linting
We are using [clippy](https://github.com/rust-lang/rust-clippy) for linting.
You can run it using:
`cargo clippy --all -- -D warnings`

## Style Guidelines

### Error handling
Any error which is returned from a public interface should be well-typed, and we use 
[thiserror](https://github.com/dtolnay/thiserror) for that purpose.

Errors returned from things only used in testing are free to use 
[anyhow](https://github.com/dtolnay/anyhow) for less verbosity.
