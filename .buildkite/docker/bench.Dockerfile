FROM cimg/rust:1.45-node AS builder
USER root

# Update rust and tools
RUN rustup default stable

# Install tools
RUN apt-get -y update
RUN apt-get -y install git

# Set env vars
ENV NPM_CONFIG_LOGLEVEL info
ENV NPM_CONFIG_FOREGROUND_SCRIPTS true

COPY bench.gitconfig /root/.gitconfig
# Clone over https as it's public and we don't have ssh keys.
RUN git clone https://github.com/temporalio/sdk-node.git /sdk-node

# Init bench
WORKDIR /sdk-node

RUN git checkout add-ns
# Initialize submodules
RUN git submodule init && git submodule update

# Build nodejs app
RUN npm ci
RUN npm run build

# Start bench test when docker image as ran.
CMD ["node", "packages/test/lib/bench.js", "--server-address", "$TEMPORAL_SERVICE_ADDRESS", "--namespace", "bench-node"]