//! Contains the protobuf definitions used as arguments to and return values from interactions with
//! the Temporal Core SDK. Language SDK authors can generate structs using the proto definitions
//! that will match the generated structs in this module.

pub mod constants;
pub mod utilities;

#[cfg(feature = "history_builders")]
mod history_builder;
#[cfg(feature = "history_builders")]
mod history_info;
mod task_token;

#[cfg(feature = "history_builders")]
pub use history_builder::{default_wes_attribs, TestHistoryBuilder, DEFAULT_WORKFLOW_TYPE};
#[cfg(feature = "history_builders")]
pub use history_info::HistoryInfo;
pub use task_token::TaskToken;

#[allow(clippy::large_enum_variant, clippy::derive_partial_eq_without_eq)]
// I'd prefer not to do this, but there are some generated things that just don't need it.
#[allow(missing_docs)]
pub mod coresdk {
    //! Contains all protobufs relating to communication between core and lang-specific SDKs

    tonic::include_proto!("coresdk");

    use crate::temporal::api::{
        common::v1::{ActivityType, Payload, Payloads, WorkflowExecution},
        failure::v1::{failure::FailureInfo, ApplicationFailureInfo, Failure},
        workflowservice::v1::PollActivityTaskQueueResponse,
    };
    use activity_task::ActivityTask;
    use serde::{Deserialize, Serialize};
    use std::{
        collections::HashMap,
        convert::TryFrom,
        fmt::{Display, Formatter},
        iter::FromIterator,
    };
    use workflow_activation::{workflow_activation_job, WorkflowActivationJob};
    use workflow_commands::{workflow_command, workflow_command::Variant, WorkflowCommand};
    use workflow_completion::{workflow_activation_completion, WorkflowActivationCompletion};

    #[allow(clippy::module_inception)]
    pub mod activity_task {
        use crate::{coresdk::ActivityTaskCompletion, task_token::fmt_tt};
        use std::fmt::{Display, Formatter};
        tonic::include_proto!("coresdk.activity_task");

        impl ActivityTask {
            pub fn cancel_from_ids(task_token: Vec<u8>, reason: ActivityCancelReason) -> Self {
                Self {
                    task_token,
                    variant: Some(activity_task::Variant::Cancel(Cancel {
                        reason: reason as i32,
                    })),
                }
            }
        }

        impl Display for ActivityTaskCompletion {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "ActivityTaskCompletion(token: {}",
                    fmt_tt(&self.task_token),
                )?;
                if let Some(r) = self.result.as_ref().and_then(|r| r.status.as_ref()) {
                    write!(f, ", {}", r)?;
                } else {
                    write!(f, ", missing result")?;
                }
                write!(f, ")")
            }
        }
    }
    #[allow(clippy::module_inception)]
    pub mod activity_result {
        tonic::include_proto!("coresdk.activity_result");
        use super::super::temporal::api::{
            common::v1::Payload,
            failure::v1::{failure, CanceledFailureInfo, Failure as APIFailure},
        };
        use crate::temporal::api::{enums::v1::TimeoutType, failure::v1::TimeoutFailureInfo};
        use activity_execution_result as aer;
        use std::fmt::{Display, Formatter};

        impl ActivityExecutionResult {
            pub const fn ok(result: Payload) -> Self {
                Self {
                    status: Some(aer::Status::Completed(Success {
                        result: Some(result),
                    })),
                }
            }

            pub fn fail(fail: APIFailure) -> Self {
                Self {
                    status: Some(aer::Status::Failed(Failure {
                        failure: Some(fail),
                    })),
                }
            }

            pub fn cancel_from_details(payload: Option<Payload>) -> Self {
                Self {
                    status: Some(aer::Status::Cancelled(Cancellation::from_details(payload))),
                }
            }

            pub const fn will_complete_async() -> Self {
                Self {
                    status: Some(aer::Status::WillCompleteAsync(WillCompleteAsync {})),
                }
            }

            pub fn is_cancelled(&self) -> bool {
                matches!(self.status, Some(aer::Status::Cancelled(_)))
            }
        }

        impl Display for aer::Status {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "ActivityExecutionResult(")?;
                match self {
                    aer::Status::Completed(v) => {
                        write!(f, "{})", v)
                    }
                    aer::Status::Failed(v) => {
                        write!(f, "{})", v)
                    }
                    aer::Status::Cancelled(v) => {
                        write!(f, "{})", v)
                    }
                    aer::Status::WillCompleteAsync(_) => {
                        write!(f, "Will complete async)")
                    }
                }
            }
        }

        impl Display for Success {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "Success(")?;
                if let Some(ref v) = self.result {
                    write!(f, "{}", v)?;
                }
                write!(f, ")")
            }
        }

        impl Display for Failure {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "Failure(")?;
                if let Some(ref v) = self.failure {
                    write!(f, "{}", v)?;
                }
                write!(f, ")")
            }
        }

        impl Display for Cancellation {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "Cancellation(")?;
                if let Some(ref v) = self.failure {
                    write!(f, "{}", v)?;
                }
                write!(f, ")")
            }
        }

        impl From<Result<Payload, APIFailure>> for ActivityExecutionResult {
            fn from(r: Result<Payload, APIFailure>) -> Self {
                Self {
                    status: match r {
                        Ok(p) => Some(aer::Status::Completed(Success { result: Some(p) })),
                        Err(f) => Some(aer::Status::Failed(Failure { failure: Some(f) })),
                    },
                }
            }
        }

        impl ActivityResolution {
            pub fn unwrap_ok_payload(self) -> Payload {
                match self.status.unwrap() {
                    activity_resolution::Status::Completed(c) => c.result.unwrap(),
                    _ => panic!("Activity was not successful"),
                }
            }

            pub fn completed_ok(&self) -> bool {
                matches!(self.status, Some(activity_resolution::Status::Completed(_)))
            }

            pub fn failed(&self) -> bool {
                matches!(self.status, Some(activity_resolution::Status::Failed(_)))
            }

            pub fn timed_out(&self) -> Option<crate::temporal::api::enums::v1::TimeoutType> {
                match self.status {
                    Some(activity_resolution::Status::Failed(Failure {
                        failure: Some(ref f),
                    })) => f
                        .is_timeout()
                        .or_else(|| f.cause.as_ref().and_then(|c| c.is_timeout())),
                    _ => None,
                }
            }

            pub fn cancelled(&self) -> bool {
                matches!(self.status, Some(activity_resolution::Status::Cancelled(_)))
            }
        }

        impl Cancellation {
            pub fn from_details(payload: Option<Payload>) -> Self {
                Cancellation {
                    failure: Some(APIFailure {
                        message: "Activity cancelled".to_string(),
                        failure_info: Some(failure::FailureInfo::CanceledFailureInfo(
                            CanceledFailureInfo {
                                details: payload.map(Into::into),
                            },
                        )),
                        ..Default::default()
                    }),
                }
            }
        }

        impl Failure {
            pub fn timeout(timeout_type: TimeoutType) -> Self {
                Failure {
                    failure: Some(APIFailure {
                        message: "Activity timed out".to_string(),
                        failure_info: Some(failure::FailureInfo::TimeoutFailureInfo(
                            TimeoutFailureInfo {
                                timeout_type: timeout_type as i32,
                                last_heartbeat_details: None,
                            },
                        )),
                        ..Default::default()
                    }),
                }
            }
        }
    }

    pub mod common {
        tonic::include_proto!("coresdk.common");
        use super::external_data::LocalActivityMarkerData;
        use crate::{
            coresdk::{AsJsonPayloadExt, IntoPayloadsExt},
            temporal::api::common::v1::{Payload, Payloads},
        };
        use std::collections::HashMap;

        pub fn build_has_change_marker_details(
            patch_id: &str,
            deprecated: bool,
        ) -> HashMap<String, Payloads> {
            let mut hm = HashMap::new();
            hm.insert("patch_id".to_string(), patch_id.as_bytes().into());
            let deprecated = deprecated as u8;
            hm.insert("deprecated".to_string(), (&[deprecated]).into());
            hm
        }

        pub fn decode_change_marker_details(
            details: &HashMap<String, Payloads>,
        ) -> Option<(String, bool)> {
            let name =
                std::str::from_utf8(&details.get("patch_id")?.payloads.first()?.data).ok()?;
            let deprecated = *details.get("deprecated")?.payloads.first()?.data.first()? != 0;
            Some((name.to_string(), deprecated))
        }

        pub fn build_local_activity_marker_details(
            metadata: LocalActivityMarkerData,
            result: Option<Payload>,
        ) -> HashMap<String, Payloads> {
            let mut hm = HashMap::new();
            // It would be more efficient for this to be proto binary, but then it shows up as
            // meaningless in the Temporal UI...
            if let Some(jsonified) = metadata.as_json_payload().into_payloads() {
                hm.insert("data".to_string(), jsonified);
            }
            if let Some(res) = result {
                hm.insert("result".to_string(), res.into());
            }
            hm
        }

        /// Given a marker detail map, returns just the local activity info, but not the payload.
        /// This is fairly inexpensive. Deserializing the whole payload may not be.
        pub fn extract_local_activity_marker_data(
            details: &HashMap<String, Payloads>,
        ) -> Option<LocalActivityMarkerData> {
            details
                .get("data")
                .and_then(|p| p.payloads.get(0))
                .and_then(|p| std::str::from_utf8(&p.data).ok())
                .and_then(|s| serde_json::from_str(s).ok())
        }

        /// Given a marker detail map, returns the local activity info and the result payload
        /// if they are found and the marker data is well-formed. This removes the data from the
        /// map.
        pub fn extract_local_activity_marker_details(
            details: &mut HashMap<String, Payloads>,
        ) -> (Option<LocalActivityMarkerData>, Option<Payload>) {
            let data = extract_local_activity_marker_data(details);
            let result = details.remove("result").and_then(|mut p| p.payloads.pop());
            (data, result)
        }
    }

    pub mod external_data {
        use prost_types::{Duration, Timestamp};
        use serde::{Deserialize, Deserializer, Serialize, Serializer};
        tonic::include_proto!("coresdk.external_data");

        // Buncha hullaballoo because prost types aren't serde compat.
        // See https://github.com/tokio-rs/prost/issues/75 which hilariously Chad opened ages ago

        #[derive(Serialize, Deserialize)]
        #[serde(remote = "Timestamp")]
        struct TimestampDef {
            pub seconds: i64,
            pub nanos: i32,
        }
        mod opt_timestamp {
            use super::*;

            pub fn serialize<S>(value: &Option<Timestamp>, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                #[derive(Serialize)]
                struct Helper<'a>(#[serde(with = "TimestampDef")] &'a Timestamp);

                value.as_ref().map(Helper).serialize(serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Timestamp>, D::Error>
            where
                D: Deserializer<'de>,
            {
                #[derive(Deserialize)]
                struct Helper(#[serde(with = "TimestampDef")] Timestamp);

                let helper = Option::deserialize(deserializer)?;
                Ok(helper.map(|Helper(external)| external))
            }
        }

        // Luckily Duration is also stored the exact same way
        #[derive(Serialize, Deserialize)]
        #[serde(remote = "Duration")]
        struct DurationDef {
            pub seconds: i64,
            pub nanos: i32,
        }
        mod opt_duration {
            use super::*;

            pub fn serialize<S>(value: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                #[derive(Serialize)]
                struct Helper<'a>(#[serde(with = "DurationDef")] &'a Duration);

                value.as_ref().map(Helper).serialize(serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
            where
                D: Deserializer<'de>,
            {
                #[derive(Deserialize)]
                struct Helper(#[serde(with = "DurationDef")] Duration);

                let helper = Option::deserialize(deserializer)?;
                Ok(helper.map(|Helper(external)| external))
            }
        }
    }

    pub mod workflow_activation {
        use crate::{
            coresdk::{
                common::NamespacedWorkflowExecution,
                workflow_activation::remove_from_cache::EvictionReason, FromPayloadsExt,
            },
            temporal::api::{
                common::v1::Header,
                history::v1::{
                    WorkflowExecutionCancelRequestedEventAttributes,
                    WorkflowExecutionSignaledEventAttributes,
                    WorkflowExecutionStartedEventAttributes,
                },
                query::v1::WorkflowQuery,
            },
        };
        use prost_types::Timestamp;
        use std::{
            collections::HashMap,
            fmt::{Display, Formatter},
        };

        tonic::include_proto!("coresdk.workflow_activation");

        pub fn create_evict_activation(
            run_id: String,
            message: String,
            reason: EvictionReason,
        ) -> WorkflowActivation {
            WorkflowActivation {
                timestamp: None,
                run_id,
                is_replaying: false,
                history_length: 0,
                jobs: vec![WorkflowActivationJob::from(
                    workflow_activation_job::Variant::RemoveFromCache(RemoveFromCache {
                        message,
                        reason: reason as i32,
                    }),
                )],
            }
        }

        pub fn query_to_job(id: String, q: WorkflowQuery) -> QueryWorkflow {
            QueryWorkflow {
                query_id: id,
                query_type: q.query_type,
                arguments: Vec::from_payloads(q.query_args),
                headers: q.header.map(|h| h.into()).unwrap_or_default(),
            }
        }

        impl WorkflowActivation {
            /// Returns the index of the eviction job if this activation contains one. If present
            /// it should always be the last job in the list.
            pub fn eviction_index(&self) -> Option<usize> {
                self.jobs.iter().position(|j| {
                    matches!(
                        j,
                        WorkflowActivationJob {
                            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_))
                        }
                    )
                })
            }

            /// Returns true if the only job is eviction
            pub fn is_only_eviction(&self) -> bool {
                self.jobs.len() == 1 && self.eviction_index().is_some()
            }

            /// Returns eviction reason if this activation has an evict job
            pub fn eviction_reason(&self) -> Option<EvictionReason> {
                self.jobs.iter().find_map(|j| {
                    if let Some(workflow_activation_job::Variant::RemoveFromCache(ref rj)) =
                        j.variant
                    {
                        EvictionReason::from_i32(rj.reason)
                    } else {
                        None
                    }
                })
            }

            /// Append an eviction job to the joblist
            pub fn append_evict_job(&mut self, evict_job: RemoveFromCache) {
                if let Some(last_job) = self.jobs.last() {
                    if matches!(
                        last_job.variant,
                        Some(workflow_activation_job::Variant::RemoveFromCache(_))
                    ) {
                        return;
                    }
                }
                let evict_job = WorkflowActivationJob::from(
                    workflow_activation_job::Variant::RemoveFromCache(evict_job),
                );
                self.jobs.push(evict_job);
            }
        }

        impl Display for EvictionReason {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self)
            }
        }

        impl Display for WorkflowActivation {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "WorkflowActivation(")?;
                write!(f, "run_id: {}, ", self.run_id)?;
                write!(f, "is_replaying: {}, ", self.is_replaying)?;
                write!(
                    f,
                    "jobs: {})",
                    self.jobs
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .as_slice()
                        .join(", ")
                )
            }
        }

        impl Display for WorkflowActivationJob {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                match &self.variant {
                    None => write!(f, "empty"),
                    Some(v) => write!(f, "{}", v),
                }
            }
        }

        impl Display for workflow_activation_job::Variant {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                match self {
                    workflow_activation_job::Variant::StartWorkflow(_) => {
                        write!(f, "StartWorkflow")
                    }
                    workflow_activation_job::Variant::FireTimer(t) => {
                        write!(f, "FireTimer({})", t.seq)
                    }
                    workflow_activation_job::Variant::UpdateRandomSeed(_) => {
                        write!(f, "UpdateRandomSeed")
                    }
                    workflow_activation_job::Variant::QueryWorkflow(_) => {
                        write!(f, "QueryWorkflow")
                    }
                    workflow_activation_job::Variant::CancelWorkflow(_) => {
                        write!(f, "CancelWorkflow")
                    }
                    workflow_activation_job::Variant::SignalWorkflow(_) => {
                        write!(f, "SignalWorkflow")
                    }
                    workflow_activation_job::Variant::ResolveActivity(r) => {
                        write!(f, "ResolveActivity({})", r.seq)
                    }
                    workflow_activation_job::Variant::NotifyHasPatch(_) => {
                        write!(f, "NotifyHasPatch")
                    }
                    workflow_activation_job::Variant::ResolveChildWorkflowExecutionStart(_) => {
                        write!(f, "ResolveChildWorkflowExecutionStart")
                    }
                    workflow_activation_job::Variant::ResolveChildWorkflowExecution(_) => {
                        write!(f, "ResolveChildWorkflowExecution")
                    }
                    workflow_activation_job::Variant::ResolveSignalExternalWorkflow(_) => {
                        write!(f, "ResolveSignalExternalWorkflow")
                    }
                    workflow_activation_job::Variant::RemoveFromCache(_) => {
                        write!(f, "RemoveFromCache")
                    }
                    workflow_activation_job::Variant::ResolveRequestCancelExternalWorkflow(_) => {
                        write!(f, "ResolveRequestCancelExternalWorkflow")
                    }
                }
            }
        }

        impl Display for QueryWorkflow {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "QueryWorkflow(id: {}, type: {})",
                    self.query_id, self.query_type
                )
            }
        }

        impl From<WorkflowExecutionSignaledEventAttributes> for SignalWorkflow {
            fn from(a: WorkflowExecutionSignaledEventAttributes) -> Self {
                Self {
                    signal_name: a.signal_name,
                    input: Vec::from_payloads(a.input),
                    identity: a.identity,
                    headers: a.header.map(Into::into).unwrap_or_default(),
                }
            }
        }

        impl From<WorkflowExecutionCancelRequestedEventAttributes> for CancelWorkflow {
            fn from(_a: WorkflowExecutionCancelRequestedEventAttributes) -> Self {
                Self { details: vec![] }
            }
        }

        /// Create a [StartWorkflow] job from corresponding event attributes
        pub fn start_workflow_from_attribs(
            attrs: WorkflowExecutionStartedEventAttributes,
            workflow_id: String,
            randomness_seed: u64,
            start_time: Timestamp,
        ) -> StartWorkflow {
            StartWorkflow {
                workflow_type: attrs.workflow_type.map(|wt| wt.name).unwrap_or_default(),
                workflow_id,
                arguments: Vec::from_payloads(attrs.input),
                randomness_seed,
                headers: match attrs.header {
                    None => HashMap::new(),
                    Some(Header { fields }) => fields,
                },
                identity: attrs.identity,
                parent_workflow_info: attrs.parent_workflow_execution.map(|pe| {
                    NamespacedWorkflowExecution {
                        namespace: attrs.parent_workflow_namespace,
                        run_id: pe.run_id,
                        workflow_id: pe.workflow_id,
                    }
                }),
                workflow_execution_timeout: attrs.workflow_execution_timeout,
                workflow_run_timeout: attrs.workflow_run_timeout,
                workflow_task_timeout: attrs.workflow_task_timeout,
                continued_from_execution_run_id: attrs.continued_execution_run_id,
                continued_initiator: attrs.initiator,
                continued_failure: attrs.continued_failure,
                last_completion_result: attrs.last_completion_result,
                first_execution_run_id: attrs.first_execution_run_id,
                retry_policy: attrs.retry_policy,
                attempt: attrs.attempt,
                cron_schedule: attrs.cron_schedule,
                workflow_execution_expiration_time: attrs.workflow_execution_expiration_time,
                cron_schedule_to_schedule_interval: attrs.first_workflow_task_backoff,
                memo: attrs.memo,
                search_attributes: attrs.search_attributes,
                start_time: Some(start_time),
            }
        }
    }

    pub mod workflow_completion {
        use crate::temporal::api::failure;
        tonic::include_proto!("coresdk.workflow_completion");

        impl workflow_activation_completion::Status {
            pub const fn is_success(&self) -> bool {
                match &self {
                    Self::Successful(_) => true,
                    Self::Failed(_) => false,
                }
            }
        }

        impl From<failure::v1::Failure> for Failure {
            fn from(f: failure::v1::Failure) -> Self {
                Failure { failure: Some(f) }
            }
        }
    }

    pub mod child_workflow {
        tonic::include_proto!("coresdk.child_workflow");
    }

    pub mod workflow_commands {
        tonic::include_proto!("coresdk.workflow_commands");

        use crate::temporal::api::{common::v1::Payloads, enums::v1::QueryResultType};
        use std::fmt::{Display, Formatter};

        impl Display for WorkflowCommand {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                match &self.variant {
                    None => write!(f, "Empty"),
                    Some(v) => write!(f, "{}", v),
                }
            }
        }

        impl Display for StartTimer {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "StartTimer({})", self.seq)
            }
        }

        impl Display for ScheduleActivity {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "ScheduleActivity({}, {})", self.seq, self.activity_type)
            }
        }

        impl Display for ScheduleLocalActivity {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "ScheduleLocalActivity({}, {})",
                    self.seq, self.activity_type
                )
            }
        }

        impl Display for QueryResult {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "RespondToQuery({})", self.query_id)
            }
        }

        impl Display for RequestCancelActivity {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "RequestCancelActivity({})", self.seq)
            }
        }

        impl Display for RequestCancelLocalActivity {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "RequestCancelLocalActivity({})", self.seq)
            }
        }

        impl Display for CancelTimer {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "CancelTimer({})", self.seq)
            }
        }

        impl Display for CompleteWorkflowExecution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "CompleteWorkflowExecution")
            }
        }

        impl Display for FailWorkflowExecution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "FailWorkflowExecution")
            }
        }

        impl Display for ContinueAsNewWorkflowExecution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "ContinueAsNewWorkflowExecution")
            }
        }

        impl Display for CancelWorkflowExecution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "CancelWorkflowExecution")
            }
        }

        impl Display for SetPatchMarker {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "SetPatchMarker({})", self.patch_id)
            }
        }

        impl Display for StartChildWorkflowExecution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "StartChildWorkflowExecution({}, {})",
                    self.seq, self.workflow_type
                )
            }
        }

        impl Display for RequestCancelExternalWorkflowExecution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "RequestCancelExternalWorkflowExecution({})", self.seq)
            }
        }

        impl Display for UpsertWorkflowSearchAttributes {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "UpsertWorkflowSearchAttributes({:?})",
                    self.search_attributes.keys()
                )
            }
        }

        impl Display for SignalExternalWorkflowExecution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "SignalExternalWorkflowExecution({})", self.seq)
            }
        }

        impl Display for CancelSignalWorkflow {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "CancelSignalWorkflow({})", self.seq)
            }
        }

        impl Display for CancelChildWorkflowExecution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "CancelChildWorkflowExecution({})",
                    self.child_workflow_seq
                )
            }
        }

        impl Display for ModifyWorkflowProperties {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "ModifyWorkflowProperties(upserted memo keys: {:?})",
                    self.upserted_memo.as_ref().map(|m| m.fields.keys())
                )
            }
        }

        impl QueryResult {
            /// Helper to construct the Temporal API query result types.
            pub fn into_components(self) -> (String, QueryResultType, Option<Payloads>, String) {
                match self {
                    QueryResult {
                        variant: Some(query_result::Variant::Succeeded(qs)),
                        query_id,
                    } => (
                        query_id,
                        QueryResultType::Answered,
                        qs.response.map(Into::into),
                        "".to_string(),
                    ),
                    QueryResult {
                        variant: Some(query_result::Variant::Failed(err)),
                        query_id,
                    } => (query_id, QueryResultType::Failed, None, err.message),
                    QueryResult {
                        variant: None,
                        query_id,
                    } => (
                        query_id,
                        QueryResultType::Failed,
                        None,
                        "Query response was empty".to_string(),
                    ),
                }
            }
        }
    }

    pub type HistoryEventId = i64;

    impl From<workflow_activation_job::Variant> for WorkflowActivationJob {
        fn from(a: workflow_activation_job::Variant) -> Self {
            Self { variant: Some(a) }
        }
    }

    impl From<Vec<WorkflowCommand>> for workflow_completion::Success {
        fn from(v: Vec<WorkflowCommand>) -> Self {
            Self { commands: v }
        }
    }

    impl From<workflow_command::Variant> for WorkflowCommand {
        fn from(v: workflow_command::Variant) -> Self {
            Self { variant: Some(v) }
        }
    }

    impl workflow_completion::Success {
        pub fn from_variants(cmds: Vec<Variant>) -> Self {
            let cmds: Vec<_> = cmds
                .into_iter()
                .map(|c| WorkflowCommand { variant: Some(c) })
                .collect();
            cmds.into()
        }
    }

    impl WorkflowActivationCompletion {
        /// Create a successful activation with no commands in it
        pub fn empty(run_id: impl Into<String>) -> Self {
            let success = workflow_completion::Success::from_variants(vec![]);
            Self {
                run_id: run_id.into(),
                status: Some(workflow_activation_completion::Status::Successful(success)),
            }
        }

        /// Create a successful activation from a list of commands
        pub fn from_cmds(run_id: impl Into<String>, cmds: Vec<workflow_command::Variant>) -> Self {
            let success = workflow_completion::Success::from_variants(cmds);
            Self {
                run_id: run_id.into(),
                status: Some(workflow_activation_completion::Status::Successful(success)),
            }
        }

        /// Create a successful activation from just one command
        pub fn from_cmd(run_id: impl Into<String>, cmd: workflow_command::Variant) -> Self {
            let success = workflow_completion::Success::from_variants(vec![cmd]);
            Self {
                run_id: run_id.into(),
                status: Some(workflow_activation_completion::Status::Successful(success)),
            }
        }

        pub fn fail(run_id: impl Into<String>, failure: Failure) -> Self {
            Self {
                run_id: run_id.into(),
                status: Some(workflow_activation_completion::Status::Failed(
                    workflow_completion::Failure {
                        failure: Some(failure),
                    },
                )),
            }
        }

        /// Returns true if the activation has either a fail, continue, cancel, or complete workflow
        /// execution command in it.
        pub fn has_execution_ending(&self) -> bool {
            self.has_complete_workflow_execution()
                || self.has_fail_execution()
                || self.has_continue_as_new()
                || self.has_cancel_workflow_execution()
        }

        /// Returns true if the activation contains a fail workflow execution command
        pub fn has_fail_execution(&self) -> bool {
            if let Some(workflow_activation_completion::Status::Successful(s)) = &self.status {
                return s.commands.iter().any(|wfc| {
                    matches!(
                        wfc,
                        WorkflowCommand {
                            variant: Some(workflow_command::Variant::FailWorkflowExecution(_)),
                        }
                    )
                });
            }
            false
        }

        /// Returns true if the activation contains a cancel workflow execution command
        pub fn has_cancel_workflow_execution(&self) -> bool {
            if let Some(workflow_activation_completion::Status::Successful(s)) = &self.status {
                return s.commands.iter().any(|wfc| {
                    matches!(
                        wfc,
                        WorkflowCommand {
                            variant: Some(workflow_command::Variant::CancelWorkflowExecution(_)),
                        }
                    )
                });
            }
            false
        }

        /// Returns true if the activation contains a continue as new workflow execution command
        pub fn has_continue_as_new(&self) -> bool {
            if let Some(workflow_activation_completion::Status::Successful(s)) = &self.status {
                return s.commands.iter().any(|wfc| {
                    matches!(
                        wfc,
                        WorkflowCommand {
                            variant: Some(
                                workflow_command::Variant::ContinueAsNewWorkflowExecution(_)
                            ),
                        }
                    )
                });
            }
            false
        }

        /// Returns true if the activation contains a complete workflow execution command
        pub fn has_complete_workflow_execution(&self) -> bool {
            if let Some(workflow_activation_completion::Status::Successful(s)) = &self.status {
                return s.commands.iter().any(|wfc| {
                    matches!(
                        wfc,
                        WorkflowCommand {
                            variant: Some(workflow_command::Variant::CompleteWorkflowExecution(_)),
                        }
                    )
                });
            }
            false
        }

        /// Returns true if the activation completion is a success with no commands
        pub fn is_empty(&self) -> bool {
            if let Some(workflow_activation_completion::Status::Successful(s)) = &self.status {
                return s.commands.is_empty();
            }
            false
        }
    }

    /// Makes converting outgoing lang commands into [WorkflowActivationCompletion]s easier
    pub trait IntoCompletion {
        /// The conversion function
        fn into_completion(self, run_id: String) -> WorkflowActivationCompletion;
    }

    impl IntoCompletion for workflow_command::Variant {
        fn into_completion(self, run_id: String) -> WorkflowActivationCompletion {
            WorkflowActivationCompletion::from_cmd(run_id, self)
        }
    }

    impl<I, V> IntoCompletion for I
    where
        I: IntoIterator<Item = V>,
        V: Into<WorkflowCommand>,
    {
        fn into_completion(self, run_id: String) -> WorkflowActivationCompletion {
            let success = self.into_iter().map(Into::into).collect::<Vec<_>>().into();
            WorkflowActivationCompletion {
                run_id,
                status: Some(workflow_activation_completion::Status::Successful(success)),
            }
        }
    }

    impl Display for WorkflowActivationCompletion {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "WorkflowActivationCompletion(run_id: {}, status: ",
                &self.run_id
            )?;
            match &self.status {
                None => write!(f, "empty")?,
                Some(s) => write!(f, "{}", s)?,
            };
            write!(f, ")")
        }
    }

    impl Display for workflow_activation_completion::Status {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                workflow_activation_completion::Status::Successful(
                    workflow_completion::Success { commands },
                ) => {
                    write!(f, "Success(")?;
                    let mut written = 0;
                    for c in commands {
                        write!(f, "{} ", c)?;
                        written += 1;
                        if written >= 10 && written < commands.len() {
                            write!(f, "... {} more", commands.len() - written)?;
                            break;
                        }
                    }
                    write!(f, ")")
                }
                workflow_activation_completion::Status::Failed(_) => {
                    write!(f, "Failed")
                }
            }
        }
    }

    impl ActivityTask {
        pub fn start_from_poll_resp(r: PollActivityTaskQueueResponse) -> Self {
            let (workflow_id, run_id) = r
                .workflow_execution
                .map(|we| (we.workflow_id, we.run_id))
                .unwrap_or_default();
            Self {
                task_token: r.task_token,
                variant: Some(activity_task::activity_task::Variant::Start(
                    activity_task::Start {
                        workflow_namespace: r.workflow_namespace,
                        workflow_type: r.workflow_type.map_or_else(|| "".to_string(), |wt| wt.name),
                        workflow_execution: Some(WorkflowExecution {
                            workflow_id,
                            run_id,
                        }),
                        activity_id: r.activity_id,
                        activity_type: r.activity_type.map_or_else(|| "".to_string(), |at| at.name),
                        header_fields: r.header.map(Into::into).unwrap_or_default(),
                        input: Vec::from_payloads(r.input),
                        heartbeat_details: Vec::from_payloads(r.heartbeat_details),
                        scheduled_time: r.scheduled_time,
                        current_attempt_scheduled_time: r.current_attempt_scheduled_time,
                        started_time: r.started_time,
                        attempt: r.attempt as u32,
                        schedule_to_close_timeout: r.schedule_to_close_timeout,
                        start_to_close_timeout: r.start_to_close_timeout,
                        heartbeat_timeout: r.heartbeat_timeout,
                        retry_policy: r.retry_policy.map(Into::into),
                        is_local: false,
                    },
                )),
            }
        }
    }

    impl From<String> for ActivityType {
        fn from(name: String) -> Self {
            Self { name }
        }
    }

    impl From<ActivityType> for String {
        fn from(at: ActivityType) -> Self {
            at.name
        }
    }

    impl Failure {
        pub fn is_timeout(&self) -> Option<crate::temporal::api::enums::v1::TimeoutType> {
            match &self.failure_info {
                Some(FailureInfo::TimeoutFailureInfo(ti)) => Some(ti.timeout_type()),
                _ => None,
            }
        }

        pub fn application_failure(message: String, non_retryable: bool) -> Self {
            Self {
                message,
                failure_info: Some(FailureInfo::ApplicationFailureInfo(
                    ApplicationFailureInfo {
                        non_retryable,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }
        }

        pub fn application_failure_from_error(ae: anyhow::Error, non_retryable: bool) -> Self {
            Self {
                failure_info: Some(FailureInfo::ApplicationFailureInfo(
                    ApplicationFailureInfo {
                        non_retryable,
                        ..Default::default()
                    },
                )),
                ..ae.chain()
                    .rfold(None, |cause, e| {
                        Some(Self {
                            message: e.to_string(),
                            cause: cause.map(Box::new),
                            ..Default::default()
                        })
                    })
                    .unwrap_or_default()
            }
        }

        /// Extracts an ApplicationFailureInfo from a Failure instance if it exists
        pub fn maybe_application_failure(&self) -> Option<&ApplicationFailureInfo> {
            if let Failure {
                failure_info: Some(FailureInfo::ApplicationFailureInfo(f)),
                ..
            } = self
            {
                Some(f)
            } else {
                None
            }
        }
    }

    impl Display for Failure {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "Failure({}, ", self.message)?;
            match self.failure_info.as_ref() {
                None => write!(f, "missing info")?,
                Some(FailureInfo::TimeoutFailureInfo(v)) => {
                    write!(f, "Timeout: {:?}", v.timeout_type())?;
                }
                Some(FailureInfo::ApplicationFailureInfo(v)) => {
                    write!(f, "Application Failure: {}", v.r#type)?;
                }
                Some(FailureInfo::CanceledFailureInfo(_)) => {
                    write!(f, "Cancelled")?;
                }
                Some(FailureInfo::TerminatedFailureInfo(_)) => {
                    write!(f, "Terminated")?;
                }
                Some(FailureInfo::ServerFailureInfo(_)) => {
                    write!(f, "Server Failure")?;
                }
                Some(FailureInfo::ResetWorkflowFailureInfo(_)) => {
                    write!(f, "Reset Workflow")?;
                }
                Some(FailureInfo::ActivityFailureInfo(v)) => {
                    write!(
                        f,
                        "Activity Failure: scheduled_event_id: {}",
                        v.scheduled_event_id
                    )?;
                }
                Some(FailureInfo::ChildWorkflowExecutionFailureInfo(v)) => {
                    write!(
                        f,
                        "Child Workflow: started_event_id: {}",
                        v.started_event_id
                    )?;
                }
            }
            write!(f, ")")
        }
    }

    impl From<&str> for Failure {
        fn from(v: &str) -> Self {
            Failure::application_failure(v.to_string(), false)
        }
    }

    impl From<String> for Failure {
        fn from(v: String) -> Self {
            Failure::application_failure(v, false)
        }
    }

    impl From<anyhow::Error> for Failure {
        fn from(ae: anyhow::Error) -> Self {
            Failure::application_failure_from_error(ae, false)
        }
    }

    pub trait FromPayloadsExt {
        fn from_payloads(p: Option<Payloads>) -> Self;
    }
    impl<T> FromPayloadsExt for T
    where
        T: FromIterator<Payload>,
    {
        fn from_payloads(p: Option<Payloads>) -> Self {
            match p {
                None => std::iter::empty().collect(),
                Some(p) => p.payloads.into_iter().map(Into::into).collect(),
            }
        }
    }

    pub trait IntoPayloadsExt {
        fn into_payloads(self) -> Option<Payloads>;
    }
    impl<T> IntoPayloadsExt for T
    where
        T: IntoIterator<Item = Payload>,
    {
        fn into_payloads(self) -> Option<Payloads> {
            let mut iterd = self.into_iter().peekable();
            if iterd.peek().is_none() {
                None
            } else {
                Some(Payloads {
                    payloads: iterd.map(Into::into).collect(),
                })
            }
        }
    }

    impl From<Payload> for Payloads {
        fn from(p: Payload) -> Self {
            Self { payloads: vec![p] }
        }
    }

    impl<T> From<T> for Payloads
    where
        T: AsRef<[u8]>,
    {
        fn from(v: T) -> Self {
            Self {
                payloads: vec![v.into()],
            }
        }
    }

    #[derive(thiserror::Error, Debug)]
    pub enum PayloadDeserializeErr {
        /// This deserializer does not handle this type of payload. Allows composing multiple
        /// deserializers.
        #[error("This deserializer does not understand this payload")]
        DeserializerDoesNotHandle,
        #[error("Error during deserialization: {0}")]
        DeserializeErr(#[from] anyhow::Error),
    }

    // TODO: Once the prototype SDK is un-prototyped this serialization will need to be compat with
    //   other SDKs (given they might execute an activity).
    pub trait AsJsonPayloadExt {
        fn as_json_payload(&self) -> anyhow::Result<Payload>;
    }
    impl<T> AsJsonPayloadExt for T
    where
        T: Serialize,
    {
        fn as_json_payload(&self) -> anyhow::Result<Payload> {
            let as_json = serde_json::to_string(self)?;
            let mut metadata = HashMap::new();
            metadata.insert("encoding".to_string(), b"json/plain".to_vec());
            Ok(Payload {
                metadata,
                data: as_json.into_bytes(),
            })
        }
    }

    pub trait FromJsonPayloadExt: Sized {
        fn from_json_payload(payload: &Payload) -> Result<Self, PayloadDeserializeErr>;
    }
    impl<T> FromJsonPayloadExt for T
    where
        T: for<'de> Deserialize<'de>,
    {
        fn from_json_payload(payload: &Payload) -> Result<Self, PayloadDeserializeErr> {
            if !matches!(
                payload.metadata.get("encoding").map(|v| v.as_slice()),
                Some(b"json/plain")
            ) {
                return Err(PayloadDeserializeErr::DeserializerDoesNotHandle);
            }
            let payload_str = std::str::from_utf8(&payload.data).map_err(anyhow::Error::from)?;
            Ok(serde_json::from_str(payload_str).map_err(anyhow::Error::from)?)
        }
    }

    /// Errors when converting from a [Payloads] api proto to our internal [Payload]
    #[derive(derive_more::Display, Debug)]
    pub enum PayloadsToPayloadError {
        MoreThanOnePayload,
        NoPayload,
    }
    impl TryFrom<Payloads> for Payload {
        type Error = PayloadsToPayloadError;

        fn try_from(mut v: Payloads) -> Result<Self, Self::Error> {
            match v.payloads.pop() {
                None => Err(PayloadsToPayloadError::NoPayload),
                Some(p) => {
                    if v.payloads.is_empty() {
                        Ok(p)
                    } else {
                        Err(PayloadsToPayloadError::MoreThanOnePayload)
                    }
                }
            }
        }
    }
}

// No need to lint these
#[allow(
    clippy::all,
    missing_docs,
    rustdoc::broken_intra_doc_links,
    rustdoc::bare_urls
)]
// This is disgusting, but unclear to me how to avoid it. TODO: Discuss w/ prost maintainer
pub mod temporal {
    pub mod api {
        pub mod batch {
            pub mod v1 {
                tonic::include_proto!("temporal.api.batch.v1");
            }
        }
        pub mod command {
            pub mod v1 {
                tonic::include_proto!("temporal.api.command.v1");

                use crate::{
                    coresdk::{workflow_commands, IntoPayloadsExt},
                    temporal::api::{
                        common::v1::{ActivityType, WorkflowType},
                        enums::v1::CommandType,
                    },
                };
                use command::Attributes;
                use std::fmt::{Display, Formatter};

                impl From<command::Attributes> for Command {
                    fn from(c: command::Attributes) -> Self {
                        match c {
                            a @ Attributes::StartTimerCommandAttributes(_) => Self {
                                command_type: CommandType::StartTimer as i32,
                                attributes: Some(a),
                            },
                            a @ Attributes::CancelTimerCommandAttributes(_) => Self {
                                command_type: CommandType::CancelTimer as i32,
                                attributes: Some(a),
                            },
                            a @ Attributes::CompleteWorkflowExecutionCommandAttributes(_) => Self {
                                command_type: CommandType::CompleteWorkflowExecution as i32,
                                attributes: Some(a),
                            },
                            a @ Attributes::FailWorkflowExecutionCommandAttributes(_) => Self {
                                command_type: CommandType::FailWorkflowExecution as i32,
                                attributes: Some(a),
                            },
                            a @ Attributes::ScheduleActivityTaskCommandAttributes(_) => Self {
                                command_type: CommandType::ScheduleActivityTask as i32,
                                attributes: Some(a),
                            },
                            a @ Attributes::RequestCancelActivityTaskCommandAttributes(_) => Self {
                                command_type: CommandType::RequestCancelActivityTask as i32,
                                attributes: Some(a),
                            },
                            a @ Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(_) => {
                                Self {
                                    command_type: CommandType::ContinueAsNewWorkflowExecution
                                        as i32,
                                    attributes: Some(a),
                                }
                            }
                            a @ Attributes::CancelWorkflowExecutionCommandAttributes(_) => Self {
                                command_type: CommandType::CancelWorkflowExecution as i32,
                                attributes: Some(a),
                            },
                            _ => unimplemented!(),
                        }
                    }
                }

                impl Display for Command {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        let ct = CommandType::from_i32(self.command_type)
                            .unwrap_or(CommandType::Unspecified);
                        write!(f, "{:?}", ct)
                    }
                }

                impl From<workflow_commands::StartTimer> for command::Attributes {
                    fn from(s: workflow_commands::StartTimer) -> Self {
                        Self::StartTimerCommandAttributes(StartTimerCommandAttributes {
                            timer_id: s.seq.to_string(),
                            start_to_fire_timeout: s.start_to_fire_timeout,
                        })
                    }
                }

                impl From<workflow_commands::UpsertWorkflowSearchAttributes> for command::Attributes {
                    fn from(s: workflow_commands::UpsertWorkflowSearchAttributes) -> Self {
                        Self::UpsertWorkflowSearchAttributesCommandAttributes(
                            UpsertWorkflowSearchAttributesCommandAttributes {
                                search_attributes: Some(s.search_attributes.into()),
                            },
                        )
                    }
                }

                impl From<workflow_commands::ModifyWorkflowProperties> for command::Attributes {
                    fn from(s: workflow_commands::ModifyWorkflowProperties) -> Self {
                        Self::ModifyWorkflowPropertiesCommandAttributes(
                            ModifyWorkflowPropertiesCommandAttributes {
                                upserted_memo: s.upserted_memo.map(Into::into),
                            },
                        )
                    }
                }

                impl From<workflow_commands::CancelTimer> for command::Attributes {
                    fn from(s: workflow_commands::CancelTimer) -> Self {
                        Self::CancelTimerCommandAttributes(CancelTimerCommandAttributes {
                            timer_id: s.seq.to_string(),
                        })
                    }
                }

                impl From<workflow_commands::ScheduleActivity> for command::Attributes {
                    fn from(s: workflow_commands::ScheduleActivity) -> Self {
                        Self::ScheduleActivityTaskCommandAttributes(
                            ScheduleActivityTaskCommandAttributes {
                                activity_id: s.activity_id,
                                activity_type: Some(ActivityType {
                                    name: s.activity_type,
                                }),
                                task_queue: Some(s.task_queue.into()),
                                header: Some(s.headers.into()),
                                input: s.arguments.into_payloads(),
                                schedule_to_close_timeout: s.schedule_to_close_timeout,
                                schedule_to_start_timeout: s.schedule_to_start_timeout,
                                start_to_close_timeout: s.start_to_close_timeout,
                                heartbeat_timeout: s.heartbeat_timeout,
                                retry_policy: s.retry_policy.map(Into::into),
                                request_eager_execution: !s.do_not_eagerly_execute,
                            },
                        )
                    }
                }

                impl From<workflow_commands::StartChildWorkflowExecution> for command::Attributes {
                    fn from(s: workflow_commands::StartChildWorkflowExecution) -> Self {
                        Self::StartChildWorkflowExecutionCommandAttributes(
                            StartChildWorkflowExecutionCommandAttributes {
                                workflow_id: s.workflow_id,
                                workflow_type: Some(WorkflowType {
                                    name: s.workflow_type,
                                }),
                                control: "".into(),
                                namespace: s.namespace,
                                task_queue: Some(s.task_queue.into()),
                                header: Some(s.headers.into()),
                                memo: Some(s.memo.into()),
                                search_attributes: Some(s.search_attributes.into()),
                                input: s.input.into_payloads(),
                                workflow_id_reuse_policy: s.workflow_id_reuse_policy,
                                workflow_execution_timeout: s.workflow_execution_timeout,
                                workflow_run_timeout: s.workflow_run_timeout,
                                workflow_task_timeout: s.workflow_task_timeout,
                                retry_policy: s.retry_policy.map(Into::into),
                                cron_schedule: s.cron_schedule.clone(),
                                parent_close_policy: s.parent_close_policy,
                            },
                        )
                    }
                }

                impl From<workflow_commands::CompleteWorkflowExecution> for command::Attributes {
                    fn from(c: workflow_commands::CompleteWorkflowExecution) -> Self {
                        Self::CompleteWorkflowExecutionCommandAttributes(
                            CompleteWorkflowExecutionCommandAttributes {
                                result: c.result.map(Into::into),
                            },
                        )
                    }
                }

                impl From<workflow_commands::FailWorkflowExecution> for command::Attributes {
                    fn from(c: workflow_commands::FailWorkflowExecution) -> Self {
                        Self::FailWorkflowExecutionCommandAttributes(
                            FailWorkflowExecutionCommandAttributes {
                                failure: c.failure.map(Into::into),
                            },
                        )
                    }
                }

                impl From<workflow_commands::ContinueAsNewWorkflowExecution> for command::Attributes {
                    fn from(c: workflow_commands::ContinueAsNewWorkflowExecution) -> Self {
                        Self::ContinueAsNewWorkflowExecutionCommandAttributes(
                            ContinueAsNewWorkflowExecutionCommandAttributes {
                                workflow_type: Some(c.workflow_type.into()),
                                task_queue: Some(c.task_queue.into()),
                                input: c.arguments.into_payloads(),
                                workflow_run_timeout: c.workflow_run_timeout,
                                workflow_task_timeout: c.workflow_task_timeout,
                                memo: if c.memo.is_empty() {
                                    None
                                } else {
                                    Some(c.memo.into())
                                },
                                header: if c.headers.is_empty() {
                                    None
                                } else {
                                    Some(c.headers.into())
                                },
                                retry_policy: c.retry_policy,
                                search_attributes: if c.search_attributes.is_empty() {
                                    None
                                } else {
                                    Some(c.search_attributes.into())
                                },
                                ..Default::default()
                            },
                        )
                    }
                }

                impl From<workflow_commands::CancelWorkflowExecution> for command::Attributes {
                    fn from(_c: workflow_commands::CancelWorkflowExecution) -> Self {
                        Self::CancelWorkflowExecutionCommandAttributes(
                            CancelWorkflowExecutionCommandAttributes { details: None },
                        )
                    }
                }
            }
        }
        pub mod common {
            pub mod v1 {
                use std::{
                    collections::HashMap,
                    fmt::{Display, Formatter},
                };
                tonic::include_proto!("temporal.api.common.v1");

                impl<T> From<T> for Payload
                where
                    T: AsRef<[u8]>,
                {
                    fn from(v: T) -> Self {
                        // TODO: Set better encodings, whole data converter deal. Setting anything
                        //  for now at least makes it show up in the web UI.
                        let mut metadata = HashMap::new();
                        metadata.insert("encoding".to_string(), b"binary/plain".to_vec());
                        Self {
                            metadata,
                            data: v.as_ref().to_vec(),
                        }
                    }
                }

                impl Payload {
                    // Is its own function b/c asref causes implementation conflicts
                    pub fn as_slice(&self) -> &[u8] {
                        self.data.as_slice()
                    }
                }

                impl Display for Payload {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        if self.data.len() > 64 {
                            let mut windows = self.data.as_slice().windows(32);
                            write!(
                                f,
                                "[{}..{}]",
                                base64::encode(windows.next().unwrap_or_default()),
                                base64::encode(windows.next_back().unwrap_or_default())
                            )
                        } else {
                            write!(f, "[{}]", base64::encode(&self.data))
                        }
                    }
                }

                impl Display for Header {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        write!(f, "Header(")?;
                        for kv in &self.fields {
                            write!(f, "{}: ", kv.0)?;
                            write!(f, "{}, ", kv.1)?;
                        }
                        write!(f, ")")
                    }
                }

                impl From<Header> for HashMap<String, Payload> {
                    fn from(h: Header) -> Self {
                        h.fields.into_iter().map(|(k, v)| (k, v.into())).collect()
                    }
                }

                impl From<Memo> for HashMap<String, Payload> {
                    fn from(h: Memo) -> Self {
                        h.fields.into_iter().map(|(k, v)| (k, v.into())).collect()
                    }
                }

                impl From<SearchAttributes> for HashMap<String, Payload> {
                    fn from(h: SearchAttributes) -> Self {
                        h.indexed_fields
                            .into_iter()
                            .map(|(k, v)| (k, v.into()))
                            .collect()
                    }
                }

                impl From<HashMap<String, Payload>> for SearchAttributes {
                    fn from(h: HashMap<String, Payload>) -> Self {
                        Self {
                            indexed_fields: h.into_iter().map(|(k, v)| (k, v.into())).collect(),
                        }
                    }
                }
            }
        }
        pub mod enums {
            pub mod v1 {
                tonic::include_proto!("temporal.api.enums.v1");
            }
        }
        pub mod failure {
            pub mod v1 {
                tonic::include_proto!("temporal.api.failure.v1");
            }
        }
        pub mod filter {
            pub mod v1 {
                tonic::include_proto!("temporal.api.filter.v1");
            }
        }
        pub mod history {
            pub mod v1 {
                use crate::temporal::api::{
                    enums::v1::EventType, history::v1::history_event::Attributes,
                };
                use anyhow::bail;
                use prost::alloc::fmt::Formatter;
                use std::fmt::Display;

                tonic::include_proto!("temporal.api.history.v1");

                impl History {
                    pub fn extract_run_id_from_start(&self) -> Result<&str, anyhow::Error> {
                        extract_original_run_id_from_events(&self.events)
                    }

                    /// Returns the event id of the final event in the history. Will return 0 if
                    /// there are no events.
                    pub fn last_event_id(&self) -> i64 {
                        self.events.last().map(|e| e.event_id).unwrap_or_default()
                    }
                }

                pub fn extract_original_run_id_from_events(
                    events: &[HistoryEvent],
                ) -> Result<&str, anyhow::Error> {
                    if let Some(Attributes::WorkflowExecutionStartedEventAttributes(wes)) =
                        events.get(0).and_then(|x| x.attributes.as_ref())
                    {
                        Ok(&wes.original_execution_run_id)
                    } else {
                        bail!("First event is not WorkflowExecutionStarted?!?")
                    }
                }

                impl HistoryEvent {
                    /// Returns true if this is an event created to mirror a command
                    pub fn is_command_event(&self) -> bool {
                        EventType::from_i32(self.event_type).map_or(false, |et| match et {
                            EventType::ActivityTaskScheduled
                            | EventType::ActivityTaskCancelRequested
                            | EventType::MarkerRecorded
                            | EventType::RequestCancelExternalWorkflowExecutionInitiated
                            | EventType::SignalExternalWorkflowExecutionInitiated
                            | EventType::StartChildWorkflowExecutionInitiated
                            | EventType::TimerCanceled
                            | EventType::TimerStarted
                            | EventType::UpsertWorkflowSearchAttributes
                            | EventType::WorkflowExecutionCanceled
                            | EventType::WorkflowExecutionCompleted
                            | EventType::WorkflowExecutionContinuedAsNew
                            | EventType::WorkflowExecutionFailed => true,
                            _ => false,
                        })
                    }

                    /// Returns the command's initiating event id, if present. This is the id of the
                    /// event which "started" the command. Usually, the "scheduled" event for the
                    /// command.
                    pub fn get_initial_command_event_id(&self) -> Option<i64> {
                        self.attributes.as_ref().and_then(|a| {
                            // Fun! Not really any way to make this better w/o incompatibly changing
                            // protos.
                            match a {
                                Attributes::ActivityTaskStartedEventAttributes(a) =>
                                    Some(a.scheduled_event_id),
                                Attributes::ActivityTaskCompletedEventAttributes(a) =>
                                    Some(a.scheduled_event_id),
                                Attributes::ActivityTaskFailedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::ActivityTaskTimedOutEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::ActivityTaskCancelRequestedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::ActivityTaskCanceledEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::TimerFiredEventAttributes(a) => Some(a.started_event_id),
                                Attributes::TimerCanceledEventAttributes(a) => Some(a.started_event_id),
                                Attributes::RequestCancelExternalWorkflowExecutionFailedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ExternalWorkflowExecutionCancelRequestedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::StartChildWorkflowExecutionFailedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionStartedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionCompletedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionFailedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionCanceledEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionTimedOutEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionTerminatedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::SignalExternalWorkflowExecutionFailedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ExternalWorkflowExecutionSignaledEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::WorkflowTaskStartedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::WorkflowTaskCompletedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::WorkflowTaskTimedOutEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::WorkflowTaskFailedEventAttributes(a) => Some(a.scheduled_event_id),
                                _ => None
                            }
                        })
                    }

                    /// Returns true if the event is one which would end a workflow
                    pub fn is_final_wf_execution_event(&self) -> bool {
                        match self.event_type() {
                            EventType::WorkflowExecutionCompleted => true,
                            EventType::WorkflowExecutionCanceled => true,
                            EventType::WorkflowExecutionFailed => true,
                            EventType::WorkflowExecutionTimedOut => true,
                            EventType::WorkflowExecutionContinuedAsNew => true,
                            EventType::WorkflowExecutionTerminated => true,
                            _ => false,
                        }
                    }
                }

                impl Display for HistoryEvent {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        write!(
                            f,
                            "HistoryEvent(id: {}, {:?})",
                            self.event_id,
                            EventType::from_i32(self.event_type)
                        )
                    }
                }
            }
        }
        pub mod interaction {
            pub mod v1 {
                tonic::include_proto!("temporal.api.interaction.v1");
            }
        }
        pub mod namespace {
            pub mod v1 {
                tonic::include_proto!("temporal.api.namespace.v1");
            }
        }
        pub mod operatorservice {
            pub mod v1 {
                tonic::include_proto!("temporal.api.operatorservice.v1");
            }
        }
        pub mod query {
            pub mod v1 {
                tonic::include_proto!("temporal.api.query.v1");
            }
        }
        pub mod replication {
            pub mod v1 {
                tonic::include_proto!("temporal.api.replication.v1");
            }
        }
        pub mod schedule {
            pub mod v1 {
                tonic::include_proto!("temporal.api.schedule.v1");
            }
        }
        pub mod taskqueue {
            pub mod v1 {
                use crate::temporal::api::enums::v1::TaskQueueKind;
                tonic::include_proto!("temporal.api.taskqueue.v1");

                impl From<String> for TaskQueue {
                    fn from(name: String) -> Self {
                        Self {
                            name,
                            kind: TaskQueueKind::Normal as i32,
                        }
                    }
                }
            }
        }
        pub mod testservice {
            pub mod v1 {
                tonic::include_proto!("temporal.api.testservice.v1");
            }
        }
        pub mod version {
            pub mod v1 {
                tonic::include_proto!("temporal.api.version.v1");
            }
        }
        pub mod workflow {
            pub mod v1 {
                tonic::include_proto!("temporal.api.workflow.v1");
            }
        }
        pub mod workflowservice {
            pub mod v1 {
                use std::{
                    convert::TryInto,
                    fmt::{Display, Formatter},
                    time::{Duration, SystemTime},
                };

                tonic::include_proto!("temporal.api.workflowservice.v1");

                macro_rules! sched_to_start_impl {
                    ($sched_field:ident) => {
                        /// Return the duration of the task schedule time (current attempt) to its
                        /// start time if both are set and time went forward.
                        pub fn sched_to_start(&self) -> Option<Duration> {
                            if let Some((sch, st)) =
                                self.$sched_field.clone().zip(self.started_time.clone())
                            {
                                let sch: Result<SystemTime, _> = sch.try_into();
                                let st: Result<SystemTime, _> = st.try_into();
                                if let (Ok(sch), Ok(st)) = (sch, st) {
                                    return st.duration_since(sch).ok();
                                }
                            }
                            None
                        }
                    };
                }

                impl PollWorkflowTaskQueueResponse {
                    sched_to_start_impl!(scheduled_time);
                }

                impl Display for PollWorkflowTaskQueueResponse {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        let last_event = self
                            .history
                            .as_ref()
                            .and_then(|h| h.events.last().map(|he| he.event_id))
                            .unwrap_or(0);
                        write!(
                            f,
                            "PollWFTQResp(run_id: {}, attempt: {}, last_event: {})",
                            self.workflow_execution
                                .as_ref()
                                .map_or("", |we| we.run_id.as_str()),
                            self.attempt,
                            last_event
                        )
                    }
                }

                /// Can be used while debugging to avoid filling up a whole screen with poll resps
                pub struct CompactHist<'a>(pub &'a PollWorkflowTaskQueueResponse);
                impl Display for CompactHist<'_> {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        writeln!(
                            f,
                            "PollWorkflowTaskQueueResponse (prev_started: {}, started: {})",
                            self.0.previous_started_event_id, self.0.started_event_id
                        )?;
                        if let Some(h) = self.0.history.as_ref() {
                            for event in &h.events {
                                writeln!(f, "{}", event)?;
                            }
                        }
                        writeln!(f, "query: {:#?}", self.0.query)?;
                        writeln!(f, "queries: {:#?}", self.0.queries)
                    }
                }

                impl PollActivityTaskQueueResponse {
                    sched_to_start_impl!(current_attempt_scheduled_time);
                }

                impl QueryWorkflowResponse {
                    /// Unwrap a successful response as vec of payloads
                    pub fn unwrap(self) -> Vec<crate::temporal::api::common::v1::Payload> {
                        self.query_result.unwrap().payloads
                    }
                }
            }
        }
    }
}

#[allow(
    clippy::all,
    missing_docs,
    rustdoc::broken_intra_doc_links,
    rustdoc::bare_urls
)]
pub mod grpc {
    pub mod health {
        pub mod v1 {
            tonic::include_proto!("grpc.health.v1");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::temporal::api::failure::v1::Failure;
    use anyhow::anyhow;

    #[test]
    fn anyhow_to_failure_conversion() {
        let no_causes: Failure = anyhow!("no causes").into();
        assert_eq!(no_causes.cause, None);
        assert_eq!(no_causes.message, "no causes");
        let orig = anyhow!("fail 1");
        let mid = orig.context("fail 2");
        let top = mid.context("fail 3");
        let as_fail: Failure = top.into();
        assert_eq!(as_fail.message, "fail 3");
        assert_eq!(as_fail.cause.as_ref().unwrap().message, "fail 2");
        assert_eq!(as_fail.cause.unwrap().cause.unwrap().message, "fail 1");
    }
}
