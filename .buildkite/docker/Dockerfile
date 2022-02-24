FROM rust:1.58

RUN rustup component add rustfmt && \
	rustup component add clippy

RUN cargo install cargo-tarpaulin

WORKDIR /sdk-core
