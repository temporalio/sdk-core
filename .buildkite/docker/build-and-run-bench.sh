#!/usr/bin/env sh
set -e
set -x

# Clone over https as it's public and we don't have ssh keys.
git clone https://github.com/temporalio/sdk-node.git /sdk-node

# TODO remove once the branch is landed on master
git -C /sdk-node checkout bench-wait-on-temporal

# Instead of initializing submodule as we would normally do, we symlink current (patched) sdk-core state.
rm -rf /sdk-node/packages/core-bridge/sdk-core
ln -s /sdk-core /sdk-node/packages/core-bridge/sdk-core

# Build bench
npm ci
npm run build

node packages/test/lib/bench.js --server-address temporal:7233