use std::{cell::Cell, fs, path::PathBuf, rc::Rc, sync::Arc};

use anyhow::{Context, bail};
use prost::Message;
use sha2::{Digest, Sha256};
use temporalio_common::protos::{
    coresdk::workflow_commands::WorkflowCommand, temporal::api::failure::v1::Failure,
};
use temporalio_workflow::runtime::{
    guest::WorkflowInstance,
    host::WorkflowHost,
    types::{
        ActivationJobResult, ActivationResult, MainRoutineCompletion, QueryResponse,
        RoutineCompletion, RoutinePollResult, StartedRoutine, TaskFailure, TerminalOutcome,
        UpdateRoutineCompletion, UpdateRoutineKind, WorkflowActivation,
        WorkflowDefinitionDescriptor, WorkflowFailure,
    },
};
use wasmtime::{
    Config, Engine, Store, StoreSnapshot,
    component::{Component, HasSelf, Linker, ResourceAny},
};

use crate::workflow_registry::{
    WorkflowDefinitions, WorkflowExecutionFactory, WorkflowExecutionInput,
};

wasmtime::component::bindgen!({
    path: "../workflow/wit",
    world: "workflow-module",
});

use self::{
    exports::temporal::workflow_runtime::workflow_guest as wit_guest,
    temporal::workflow_runtime::{types as wit_types, workflow_host as wit_host},
};

const SNAPSHOT_FORMAT_VERSION: u32 = 1;
const SYNC_WIT_ABI_VERSION: &str = "temporal:workflow-runtime@0.1.0/sync-poll";
const WASMTIME_COMPATIBILITY_KEY: &str =
    "wasmtime-44.0.1+temporal-snapshot-poc/component-model/default-config";
const DEFAULT_SNAPSHOT_CONTEXT_POOL_SIZE: usize = 2;
const WORKFLOW_INSTANCE_RESOURCE_ID: u64 = 0;
const WORKFLOW_INSTANCE_RESOURCE_TYPE: &str =
    "temporal:workflow-runtime/workflow-guest.workflow-instance";
#[allow(dead_code)]
const WASM_PAGE_SIZE: usize = 64 * 1024;

/// Options for the experimental WASM workflow snapshot proof-of-concept.
#[derive(Clone, Debug)]
pub struct WasmWorkflowSnapshotOptions {
    enabled: bool,
    context_pool_size: usize,
}

impl Default for WasmWorkflowSnapshotOptions {
    fn default() -> Self {
        Self::disabled()
    }
}

impl WasmWorkflowSnapshotOptions {
    /// Return options with snapshotting disabled.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            context_pool_size: DEFAULT_SNAPSHOT_CONTEXT_POOL_SIZE,
        }
    }

    /// Return options with snapshotting enabled and the default context pool size.
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            context_pool_size: DEFAULT_SNAPSHOT_CONTEXT_POOL_SIZE,
        }
    }

    /// Set the maximum number of active WASM execution contexts.
    pub fn with_context_pool_size(mut self, context_pool_size: usize) -> Self {
        self.context_pool_size = context_pool_size.max(1);
        self
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[derive(Debug)]
pub(crate) struct WasmRuntimeServices {
    snapshot_options: WasmWorkflowSnapshotOptions,
    context_pool: WasmExecutionContextPool,
}

impl WasmRuntimeServices {
    pub(crate) fn new(snapshot_options: WasmWorkflowSnapshotOptions) -> Self {
        Self {
            context_pool: WasmExecutionContextPool::new(snapshot_options.context_pool_size),
            snapshot_options,
        }
    }

    fn snapshot_options(&self) -> &WasmWorkflowSnapshotOptions {
        &self.snapshot_options
    }
}

#[derive(Debug)]
struct WasmExecutionContextPool {
    capacity: usize,
    leased: Cell<usize>,
}

impl WasmExecutionContextPool {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            leased: Cell::new(0),
        }
    }

    fn try_acquire(
        runtime: &Rc<WasmRuntimeServices>,
    ) -> Result<WasmExecutionContextLease, anyhow::Error> {
        let leased = runtime.context_pool.leased.get();
        if leased >= runtime.context_pool.capacity {
            bail!(
                "WASM workflow snapshot execution context pool exhausted: {leased}/{} leased",
                runtime.context_pool.capacity
            );
        }
        runtime.context_pool.leased.set(leased + 1);
        Ok(WasmExecutionContextLease {
            runtime: runtime.clone(),
        })
    }

    fn release(&self) {
        let leased = self.leased.get();
        debug_assert!(leased > 0, "releasing an unleased WASM execution context");
        self.leased.set(leased.saturating_sub(1));
    }
}

#[derive(Debug)]
struct WasmExecutionContextLease {
    runtime: Rc<WasmRuntimeServices>,
}

impl Drop for WasmExecutionContextLease {
    fn drop(&mut self) {
        self.runtime.context_pool.release();
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WasmSnapshotCapabilityFlags {
    sync_boundary_only: bool,
    guest_threads_or_shared_memory: bool,
    sparse_host_resources: bool,
}

impl Default for WasmSnapshotCapabilityFlags {
    fn default() -> Self {
        Self {
            sync_boundary_only: true,
            guest_threads_or_shared_memory: false,
            sparse_host_resources: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WasmSnapshotMetadata {
    snapshot_format_version: u32,
    wit_abi_version: String,
    wasmtime_compatibility_key: String,
    component_id: String,
    workflow_type: String,
    run_id: String,
    source_wasm_sha256: String,
    compiled_component_key: String,
    capabilities: WasmSnapshotCapabilityFlags,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WasmWorkflowSnapshot {
    metadata: WasmSnapshotMetadata,
    store: StoreSnapshot,
    resources: Vec<WasmResourceSnapshot>,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
struct SparseMemorySnapshot {
    memory_index: u32,
    current_pages: u64,
    nonzero_pages: Vec<SparseMemoryPage>,
}

#[cfg(test)]
impl SparseMemorySnapshot {
    fn from_memory_bytes(memory_index: u32, current_pages: u64, bytes: &[u8]) -> Self {
        let mut nonzero_pages = Vec::new();
        for (page_index, chunk) in bytes.chunks(WASM_PAGE_SIZE).enumerate() {
            if chunk.iter().any(|byte| *byte != 0) {
                nonzero_pages.push(SparseMemoryPage {
                    page_index: page_index as u64,
                    bytes: chunk.to_vec(),
                });
            }
        }
        Self {
            memory_index,
            current_pages,
            nonzero_pages,
        }
    }

    #[allow(dead_code)]
    fn restore_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![0; self.current_pages as usize * WASM_PAGE_SIZE];
        for page in &self.nonzero_pages {
            let start = page.page_index as usize * WASM_PAGE_SIZE;
            let end = start + page.bytes.len();
            bytes[start..end].copy_from_slice(&page.bytes);
        }
        bytes
    }
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
struct SparseMemoryPage {
    page_index: u64,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WasmResourceSnapshot {
    resource_id: u64,
    type_name: String,
    resource_rep: u32,
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

/// A prebuilt WebAssembly component that exports one or more Temporal workflows.
#[derive(Clone, Debug)]
pub struct WasmWorkflowComponent {
    component_id: String,
    source: WasmWorkflowComponentSource,
}

#[derive(Clone, Debug)]
enum WasmWorkflowComponentSource {
    File(PathBuf),
    Bytes(Arc<[u8]>),
}

impl WasmWorkflowComponent {
    /// Register a workflow component from a component file on disk.
    pub fn from_file(
        component_id: impl Into<String>,
        path: impl Into<PathBuf>,
    ) -> Result<Self, anyhow::Error> {
        let path = path.into();
        if !path.exists() {
            bail!(
                "WASM workflow component file does not exist: {}",
                path.display()
            );
        }
        Ok(Self {
            component_id: component_id.into(),
            source: WasmWorkflowComponentSource::File(path),
        })
    }

    /// Register a workflow component from in-memory bytes.
    pub fn from_bytes(
        component_id: impl Into<String>,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Result<Self, anyhow::Error> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            bail!("WASM workflow component bytes must not be empty");
        }
        Ok(Self {
            component_id: component_id.into(),
            source: WasmWorkflowComponentSource::Bytes(bytes),
        })
    }
}

impl WorkflowDefinitions {
    pub(crate) fn register_wasm_workflows(
        &mut self,
        components: Vec<WasmWorkflowComponent>,
    ) -> Result<(), anyhow::Error> {
        for component in components {
            let module = Arc::new(CompiledWasmWorkflowModule::new(component)?);
            for definition in &module.definitions {
                let module = module.clone();
                let factory: WorkflowExecutionFactory =
                    Arc::new(move |input| module.instantiate(input));
                self.insert_workflow(definition.clone(), factory)?;
            }
        }
        Ok(())
    }
}

struct CompiledWasmWorkflowModule {
    engine: Engine,
    component: Component,
    component_id: String,
    source_wasm_sha256: String,
    compiled_component_key: String,
    definitions: Vec<WorkflowDefinitionDescriptor>,
}

impl CompiledWasmWorkflowModule {
    fn new(component: WasmWorkflowComponent) -> Result<Self, anyhow::Error> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        let engine = Engine::new(&config)?;
        let bytes: Arc<[u8]> = match &component.source {
            WasmWorkflowComponentSource::File(path) => fs::read(path)
                .with_context(|| format!("Failed reading WASM component {}", path.display()))?
                .into(),
            WasmWorkflowComponentSource::Bytes(bytes) => bytes.clone(),
        };
        let component_id = component.component_id;
        let source_wasm_sha256 = sha256_hex(bytes.as_ref());
        let compiled_component_key =
            format!("{WASMTIME_COMPATIBILITY_KEY}/source-sha256:{source_wasm_sha256}");
        let component = Component::new(&engine, bytes.as_ref()).map_err(|err| {
            anyhow::Error::msg(format!(
                "Failed to compile WASM component {component_id}: {err}"
            ))
        })?;
        let definitions = Self::read_definitions(&engine, &component)?;
        if definitions.is_empty() {
            bail!("WASM component exports no workflows");
        }
        Ok(Self {
            engine,
            component,
            component_id,
            source_wasm_sha256,
            compiled_component_key,
            definitions,
        })
    }

    fn read_definitions(
        engine: &Engine,
        component: &Component,
    ) -> Result<Vec<WorkflowDefinitionDescriptor>, anyhow::Error> {
        let mut linker = Linker::new(engine);
        WorkflowModule::add_to_linker::<_, HasSelf<_>>(&mut linker, |data| data)?;
        let mut store = Store::new(
            engine,
            WasmWorkflowHostState::new(Rc::new(NoopWorkflowHost)),
        );
        let module = WorkflowModule::instantiate(&mut store, component, &linker)?;
        module
            .temporal_workflow_runtime_workflow_guest()
            .call_list_workflows(&mut store)
            .map(|defs| {
                defs.into_iter()
                    .map(|def| WorkflowDefinitionDescriptor {
                        workflow_type: def.workflow_type,
                        has_init: def.has_init,
                        init_takes_input: def.init_takes_input,
                        signals: def.signals,
                        queries: def.queries,
                        updates: def
                            .updates
                            .into_iter()
                            .map(|u| {
                                temporalio_workflow::runtime::types::UpdateDefinitionDescriptor {
                                    name: u.name,
                                    has_validator: u.has_validator,
                                }
                            })
                            .collect(),
                    })
                    .collect()
            })
            .map_err(|err| {
                anyhow::Error::msg(format!(
                    "Failed to list workflows exported by WASM component: {err}"
                ))
            })
    }

    fn instantiate(
        self: &Arc<Self>,
        input: WorkflowExecutionInput,
    ) -> Result<Box<dyn WorkflowInstance>, anyhow::Error> {
        let snapshot_options = input
            .wasm_runtime
            .as_ref()
            .map(|runtime| runtime.snapshot_options().clone())
            .unwrap_or_else(WasmWorkflowSnapshotOptions::disabled);
        let context_lease = if snapshot_options.is_enabled() {
            let runtime = input
                .wasm_runtime
                .as_ref()
                .context("WASM workflow snapshotting requires runtime services")?;
            Some(WasmExecutionContextPool::try_acquire(runtime)?)
        } else {
            None
        };
        let rehydrate_input = WasmWorkflowRehydrateInput {
            namespace: input.namespace.clone(),
            task_queue: input.task_queue.clone(),
            run_id: input.run_id.clone(),
            workflow_type: input.init_workflow_job.workflow_type.clone(),
            initialize_workflow: input.init_workflow_job.encode_to_vec(),
            host: input.host.clone(),
            runtime: input.wasm_runtime.clone(),
        };
        let snapshot_metadata = self.snapshot_metadata(&rehydrate_input);
        let live = self.instantiate_live_context(&rehydrate_input, input.host, context_lease)?;

        Ok(Box::new(WasmWorkflowInstance {
            module: self.clone(),
            live: Some(live),
            snapshot_metadata,
            snapshot_options,
            rehydrate_input,
            last_snapshot: None,
        }))
    }

    fn snapshot_metadata(&self, input: &WasmWorkflowRehydrateInput) -> WasmSnapshotMetadata {
        WasmSnapshotMetadata {
            snapshot_format_version: SNAPSHOT_FORMAT_VERSION,
            wit_abi_version: SYNC_WIT_ABI_VERSION.to_string(),
            wasmtime_compatibility_key: WASMTIME_COMPATIBILITY_KEY.to_string(),
            component_id: self.component_id.clone(),
            workflow_type: input.workflow_type.clone(),
            run_id: input.run_id.clone(),
            source_wasm_sha256: self.source_wasm_sha256.clone(),
            compiled_component_key: self.compiled_component_key.clone(),
            capabilities: WasmSnapshotCapabilityFlags::default(),
        }
    }

    fn instantiate_live_context(
        &self,
        input: &WasmWorkflowRehydrateInput,
        host: Rc<dyn WorkflowHost>,
        context_lease: Option<WasmExecutionContextLease>,
    ) -> Result<WasmLiveWorkflowContext, anyhow::Error> {
        let mut linker = Linker::new(&self.engine);
        WorkflowModule::add_to_linker::<_, HasSelf<_>>(&mut linker, |data| data)?;
        let mut store = Store::new(&self.engine, WasmWorkflowHostState::new(host));
        let module = WorkflowModule::instantiate(&mut store, &self.component, &linker)?;
        let guest = module.temporal_workflow_runtime_workflow_guest();
        let workflow_init = wit_types::WorkflowInit {
            namespace: input.namespace.clone(),
            task_queue: input.task_queue.clone(),
            run_id: input.run_id.clone(),
            initialize_workflow: input.initialize_workflow.clone(),
        };
        let workflow_instance = guest
            .call_instantiate_workflow(&mut store, &workflow_init)
            .map_err(|err| {
                anyhow::Error::msg(format!("Failed to instantiate WASM workflow: {err}"))
            })?
            .map_err(convert_failure)
            .map_err(|failure| {
                anyhow::Error::msg(format!(
                    "WASM workflow initialization failed: {}",
                    failure.message
                ))
            })?;

        Ok(WasmLiveWorkflowContext {
            store,
            guest: guest.clone(),
            workflow_instance,
            _context_lease: context_lease,
        })
    }
}

struct WasmWorkflowRehydrateInput {
    namespace: String,
    task_queue: String,
    run_id: String,
    workflow_type: String,
    initialize_workflow: Vec<u8>,
    host: Rc<dyn WorkflowHost>,
    runtime: Option<Rc<WasmRuntimeServices>>,
}

struct WasmWorkflowInstance {
    module: Arc<CompiledWasmWorkflowModule>,
    live: Option<WasmLiveWorkflowContext>,
    snapshot_metadata: WasmSnapshotMetadata,
    snapshot_options: WasmWorkflowSnapshotOptions,
    rehydrate_input: WasmWorkflowRehydrateInput,
    last_snapshot: Option<WasmWorkflowSnapshot>,
}

struct WasmLiveWorkflowContext {
    store: Store<WasmWorkflowHostState>,
    guest: wit_guest::Guest,
    workflow_instance: ResourceAny,
    _context_lease: Option<WasmExecutionContextLease>,
}

impl WorkflowInstance for WasmWorkflowInstance {
    fn activate(
        &mut self,
        activation: WorkflowActivation,
    ) -> Result<ActivationResult, WorkflowFailure> {
        let live = self.live_mut()?;
        let result = live.guest.workflow_instance().call_activate(
            &mut live.store,
            live.workflow_instance,
            &activation.encode_to_vec(),
        );
        trap_to_failure(result, |result| ActivationResult {
            job_results: result
                .job_results
                .into_iter()
                .map(|job_result| match job_result {
                    wit_types::ActivationJobResult::None => ActivationJobResult::None,
                    wit_types::ActivationJobResult::StartedRoutine(routine) => {
                        ActivationJobResult::StartedRoutine(StartedRoutine {
                            routine_id: routine.routine_id,
                            kind: match routine.kind {
                                wit_types::RoutineKind::Main => {
                                    temporalio_workflow::runtime::types::RoutineKind::Main
                                }
                                wit_types::RoutineKind::Signal(name) => {
                                    temporalio_workflow::runtime::types::RoutineKind::Signal(name)
                                }
                                wit_types::RoutineKind::Update(update) => {
                                    temporalio_workflow::runtime::types::RoutineKind::Update(
                                        UpdateRoutineKind {
                                            name: update.name,
                                            update_id: update.update_id,
                                            protocol_instance_id: update.protocol_instance_id,
                                        },
                                    )
                                }
                            },
                        })
                    }
                    wit_types::ActivationJobResult::QueryResponse(response) => {
                        ActivationJobResult::QueryResponse(Box::new(QueryResponse {
                            result: response
                                .response
                                .map(decode_proto)
                                .map_err(|failure| *convert_failure(failure)),
                        }))
                    }
                    wit_types::ActivationJobResult::UpdateRejected(failure) => {
                        ActivationJobResult::UpdateRejected(convert_failure(failure))
                    }
                })
                .collect(),
        })
    }

    fn poll_routine(
        &mut self,
        routine_id: u64,
        _waker: &std::task::Waker,
    ) -> Result<RoutinePollResult, WorkflowFailure> {
        let live = self.live_mut()?;
        let result = live.guest.workflow_instance().call_poll_routine(
            &mut live.store,
            live.workflow_instance,
            routine_id,
        );
        trap_to_failure(result, |result| RoutinePollResult {
            completion: result.completion.map(|completion| match completion {
                wit_types::RoutineCompletion::Main(completion) => {
                    RoutineCompletion::Main(match completion {
                        wit_types::MainRoutineCompletion::Blocked => MainRoutineCompletion::Blocked,
                        wit_types::MainRoutineCompletion::TaskFailed(task_failure) => {
                            MainRoutineCompletion::TaskFailed(TaskFailure {
                                failure: convert_failure(task_failure.failure),
                                force_cause: task_failure.force_cause,
                            })
                        }
                        wit_types::MainRoutineCompletion::Terminal(outcome) => {
                            MainRoutineCompletion::Terminal(Box::new(match outcome {
                                wit_types::TerminalOutcome::Completed(payload) => {
                                    TerminalOutcome::Completed(decode_proto(payload))
                                }
                                wit_types::TerminalOutcome::Failed(failure) => {
                                    TerminalOutcome::Failed(convert_failure(failure))
                                }
                                wit_types::TerminalOutcome::Cancelled => TerminalOutcome::Cancelled,
                                wit_types::TerminalOutcome::ContinueAsNew(req) => {
                                    TerminalOutcome::ContinueAsNew(Box::new(decode_proto(req)))
                                }
                            }))
                        }
                    })
                }
                wit_types::RoutineCompletion::Signal(result) => {
                    RoutineCompletion::Signal(match result {
                        wit_types::SignalRoutineCompletion::Succeeded => Ok(()),
                        wit_types::SignalRoutineCompletion::Failed(failure) => {
                            Err(convert_failure(failure))
                        }
                    })
                }
                wit_types::RoutineCompletion::Update(completion) => {
                    RoutineCompletion::Update(match completion {
                        wit_types::UpdateRoutineCompletion::Completed(success) => {
                            UpdateRoutineCompletion::Completed {
                                protocol_instance_id: success.protocol_instance_id,
                                result: decode_proto(success.value),
                            }
                        }
                        wit_types::UpdateRoutineCompletion::Rejected(rejection) => {
                            UpdateRoutineCompletion::Rejected {
                                protocol_instance_id: rejection.protocol_instance_id,
                                failure: convert_failure(rejection.failure),
                            }
                        }
                    })
                }
            }),
            made_progress: result.made_progress,
        })
    }

    fn checkpoint_activation(&mut self) -> Result<(), WorkflowFailure> {
        if !self.snapshot_options.is_enabled() {
            return Ok(());
        }

        match self.snapshot_quiescent() {
            Ok(snapshot) => {
                self.last_snapshot = Some(snapshot);
                self.live.take();
                Ok(())
            }
            Err(err) => Err(Box::new(Failure {
                message: err.to_string(),
                ..Default::default()
            })),
        }
    }
}

impl WasmWorkflowInstance {
    fn live_mut(&mut self) -> Result<&mut WasmLiveWorkflowContext, WorkflowFailure> {
        if self.live.is_none() {
            self.rehydrate_live_context().map_err(|err| {
                Box::new(Failure {
                    message: err.to_string(),
                    ..Default::default()
                })
            })?;
        }

        self.live.as_mut().ok_or_else(|| {
            Box::new(Failure {
                message:
                    "WASM workflow snapshot rehydration did not produce a live execution context"
                        .to_string(),
                ..Default::default()
            })
        })
    }

    fn snapshot_quiescent(&mut self) -> Result<WasmWorkflowSnapshot, anyhow::Error> {
        let live = self
            .live
            .as_mut()
            .context("WASM workflow has no live execution context to snapshot")?;
        let store = live.store.snapshot().map_err(|err| {
            anyhow::Error::msg(format!("failed to snapshot WASM workflow store: {err}"))
        })?;
        let workflow_instance_rep = live
            .workflow_instance
            .resource_rep(&mut live.store)
            .map_err(|err| {
                anyhow::Error::msg(format!("failed to snapshot WASM workflow resource: {err}"))
            })?;
        Ok(WasmWorkflowSnapshot {
            metadata: self.snapshot_metadata.clone(),
            store,
            resources: vec![WasmResourceSnapshot {
                resource_id: WORKFLOW_INSTANCE_RESOURCE_ID,
                type_name: WORKFLOW_INSTANCE_RESOURCE_TYPE.to_string(),
                resource_rep: workflow_instance_rep,
            }],
        })
    }

    fn rehydrate_live_context(&mut self) -> Result<(), anyhow::Error> {
        let snapshot = self
            .last_snapshot
            .as_ref()
            .context("WASM workflow has no snapshot to rehydrate from")?;
        self.ensure_snapshot_compatible(snapshot)?;
        let context_lease = if self.snapshot_options.is_enabled() {
            let runtime = self
                .rehydrate_input
                .runtime
                .as_ref()
                .context("WASM workflow snapshot rehydration requires runtime services")?;
            Some(WasmExecutionContextPool::try_acquire(runtime)?)
        } else {
            None
        };
        let noop_host: Rc<dyn WorkflowHost> = Rc::new(NoopWorkflowHost);
        let mut live = self.module.instantiate_live_context(
            &self.rehydrate_input,
            noop_host,
            context_lease,
        )?;
        live.store
            .restore_snapshot(&snapshot.store)
            .map_err(|err| {
                anyhow::Error::msg(format!(
                    "failed to restore WASM workflow store snapshot: {err}"
                ))
            })?;
        let workflow_instance_resource = self.workflow_instance_resource(snapshot)?;
        live.workflow_instance
            .set_resource_rep(&mut live.store, workflow_instance_resource.resource_rep)
            .map_err(|err| {
                anyhow::Error::msg(format!(
                    "failed to restore WASM workflow resource snapshot: {err}"
                ))
            })?;
        live.store.data_mut().host = self.rehydrate_input.host.clone();
        self.live = Some(live);
        Ok(())
    }

    fn ensure_snapshot_compatible(
        &self,
        snapshot: &WasmWorkflowSnapshot,
    ) -> Result<(), anyhow::Error> {
        let current = &self.snapshot_metadata;
        let metadata = &snapshot.metadata;
        if metadata.snapshot_format_version != SNAPSHOT_FORMAT_VERSION {
            bail!(
                "unsupported WASM workflow snapshot format version {}",
                metadata.snapshot_format_version
            );
        }
        if metadata.wit_abi_version != current.wit_abi_version
            || metadata.wasmtime_compatibility_key != current.wasmtime_compatibility_key
            || metadata.component_id != current.component_id
            || metadata.workflow_type != current.workflow_type
            || metadata.run_id != current.run_id
            || metadata.source_wasm_sha256 != current.source_wasm_sha256
            || metadata.compiled_component_key != current.compiled_component_key
        {
            bail!(
                "WASM workflow snapshot metadata does not match the currently registered component"
            );
        }
        self.workflow_instance_resource(snapshot)?;
        Ok(())
    }

    fn workflow_instance_resource<'a>(
        &self,
        snapshot: &'a WasmWorkflowSnapshot,
    ) -> Result<&'a WasmResourceSnapshot, anyhow::Error> {
        if snapshot.resources.len() != 1 {
            bail!(
                "WASM workflow snapshot must contain exactly one workflow-instance resource, found {}",
                snapshot.resources.len()
            );
        }
        let resource = &snapshot.resources[0];
        if resource.resource_id != WORKFLOW_INSTANCE_RESOURCE_ID
            || resource.type_name != WORKFLOW_INSTANCE_RESOURCE_TYPE
        {
            bail!("WASM workflow snapshot contains an unsupported resource");
        }
        Ok(resource)
    }
}

struct WasmWorkflowHostState {
    host: Rc<dyn WorkflowHost>,
}

impl WasmWorkflowHostState {
    fn new(host: Rc<dyn WorkflowHost>) -> Self {
        Self { host }
    }
}

impl wit_types::Host for WasmWorkflowHostState {}

impl wit_host::Host for WasmWorkflowHostState {
    fn set_current_details(&mut self, details: String) {
        self.host.set_current_details(details);
    }

    fn push_command(&mut self, command: wit_types::WorkflowCommand) {
        self.host.push_command(decode_proto(command));
    }
}

struct NoopWorkflowHost;

impl WorkflowHost for NoopWorkflowHost {
    fn set_current_details(&self, _details: String) {}
    fn push_command(&self, _command: WorkflowCommand) {}
}

fn trap_to_failure<T, U>(
    result: Result<Result<T, wit_types::Failure>, wasmtime::Error>,
    convert: impl FnOnce(T) -> U,
) -> Result<U, WorkflowFailure> {
    result
        .map_err(|trap| {
            Box::new(Failure {
                message: format!("WASM workflow trapped: {trap}"),
                ..Default::default()
            })
        })?
        .map(convert)
        .map_err(convert_failure)
}

fn convert_failure(failure: wit_types::Failure) -> WorkflowFailure {
    Box::new(decode_proto(failure))
}

fn decode_proto<M: Message + prost::Name + Default>(bytes: Vec<u8>) -> M {
    M::decode(bytes.as_slice()).unwrap_or_else(|err| {
        let n = M::NAME;
        panic!("failed to decode {n} from WASM boundary bytes: {err}")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::{Instance, Module, Val};

    #[test]
    fn sha256_hex_uses_expected_encoding() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn sparse_memory_snapshot_omits_zero_pages_and_restores_bytes() {
        let mut memory = vec![0; WASM_PAGE_SIZE * 3];
        memory[7] = 1;
        memory[WASM_PAGE_SIZE * 2 + 13] = 2;

        let snapshot = SparseMemorySnapshot::from_memory_bytes(0, 3, &memory);

        assert_eq!(snapshot.current_pages, 3);
        assert_eq!(snapshot.nonzero_pages.len(), 2);
        assert_eq!(snapshot.nonzero_pages[0].page_index, 0);
        assert_eq!(snapshot.nonzero_pages[1].page_index, 2);
        assert_eq!(snapshot.restore_bytes(), memory);
    }

    #[test]
    fn snapshot_options_clamp_pool_size() {
        let opts = WasmWorkflowSnapshotOptions::enabled().with_context_pool_size(0);
        let runtime = Rc::new(WasmRuntimeServices::new(opts));

        let _lease = WasmExecutionContextPool::try_acquire(&runtime).unwrap();
        assert!(WasmExecutionContextPool::try_acquire(&runtime).is_err());
    }

    #[test]
    fn vendored_wasmtime_snapshot_restores_store_state() {
        let engine = Engine::default();
        let module = Module::new(
            &engine,
            r#"
            (module
                (memory (export "memory") 1 2)
                (global (export "g") (mut i32) (i32.const 7))
                (func (export "write") (param i32) (param i32)
                    local.get 0
                    local.get 1
                    i32.store8)
            )
            "#,
        )
        .unwrap();

        let mut source_store = Store::new(&engine, ());
        let source_instance = Instance::new(&mut source_store, &module, &[]).unwrap();
        source_instance
            .get_typed_func::<(i32, i32), ()>(&mut source_store, "write")
            .unwrap()
            .call(&mut source_store, (17, 99))
            .unwrap();
        source_instance
            .get_global(&mut source_store, "g")
            .unwrap()
            .set(&mut source_store, Val::I32(42))
            .unwrap();

        let snapshot = source_store.snapshot().unwrap();
        assert_eq!(snapshot.memories[0].nonzero_pages.len(), 1);

        let mut target_store = Store::new(&engine, ());
        let target_instance = Instance::new(&mut target_store, &module, &[]).unwrap();
        target_store.restore_snapshot(&snapshot).unwrap();

        let target_memory = target_instance
            .get_memory(&mut target_store, "memory")
            .unwrap();
        assert_eq!(target_memory.data(&target_store)[17], 99);
        assert_eq!(
            target_instance
                .get_global(&mut target_store, "g")
                .unwrap()
                .get(&mut target_store)
                .i32()
                .unwrap(),
            42
        );
    }
}
