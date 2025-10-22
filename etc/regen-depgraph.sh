# Run this from the repo root
cargo depgraph \
  --focus temporalio-sdk,temporalio-common,temporalio-client,temporalio-sdk-core,rustfsm,temporalio-sdk-core-c-bridge \
  --dev-deps \
  | dot -Tsvg > arch_docs/diagrams/deps.svg