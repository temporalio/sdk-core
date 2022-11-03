FROM rust:1.65

RUN rustup component add rustfmt && \
	rustup component add clippy

RUN cargo install cargo-tarpaulin
RUN apt-get update && apt-get install -y protobuf-compiler

WORKDIR /sdk-core
