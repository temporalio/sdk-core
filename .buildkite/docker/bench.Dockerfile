FROM cimg/rust:1.53-node AS builder
USER root

# Update rust and tools
RUN rustup default stable

# Install tools
RUN apt-get -y update
RUN apt-get -y install git

# Set env vars
ENV NPM_CONFIG_LOGLEVEL info
ENV NPM_CONFIG_FOREGROUND_SCRIPTS true

COPY .buildkite/docker/build-and-run-bench.sh /usr/bin/build-and-run-bench.sh

WORKDIR /sdk-node