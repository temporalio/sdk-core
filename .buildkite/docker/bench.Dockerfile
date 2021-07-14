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

COPY .buildkite/docker/bench.gitconfig /root/.gitconfig
COPY .buildkite/docker/build-bench.sh /usr/bin/build-bench.sh
# Clone over https as it's public and we don't have ssh keys.
RUN git clone https://github.com/temporalio/sdk-node.git /sdk-node

# Instead of initializing submodule as we would normally do, we make a copy of the current (patched) sdk-core state.
COPY . /sdk-node/packages/core-bridge/sdk-core

# Build bench
WORKDIR /sdk-node
RUN build-bench.sh
