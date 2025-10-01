# Run this from the repo root
cargo depgraph \
  --focus temporal-sdk,temporal-sdk-core-protos,temporal-client,temporal-sdk-core-api,temporal-sdk-core,rustfsm,temporal-sdk-core-c-bridge \
  --dev-deps \
  | dot -Tsvg > etc/deps.svg
