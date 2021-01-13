Core SDK that can be used as a base for all other SDKs.

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
