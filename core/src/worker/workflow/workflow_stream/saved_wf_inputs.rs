use crate::{
    telemetry::metrics::MetricsContext,
    worker::{
        client::mocks::DEFAULT_TEST_CAPABILITIES,
        workflow::{
            workflow_stream::{WFStream, WFStreamInput},
            LAReqSink, LocalActivityRequestSink,
        },
        LocalActRequest, LocalActivityResolution,
    },
};
use crossbeam::queue::SegQueue;
use futures::Stream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::{future, sync::Arc};
use temporal_sdk_core_api::worker::WorkerConfig;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

/// Replay everything that happened to internal workflow state. Useful for 100% deterministic
/// reproduction of bugs.
///
/// Use `CoreWfStarter::enable_wf_state_input_recording` from the integration test utilities to
/// activate saving the data to disk, and use the `wf_input_replay` example binary to replay.
pub async fn replay_wf_state_inputs(mut config: WorkerConfig, inputs: impl Stream<Item = Vec<u8>>) {
    use crate::worker::build_wf_basics;

    let la_resp_q = Arc::new(SegQueue::new());
    let la_resp_q_clone = la_resp_q.clone();
    let inputs = inputs
        .map(|bytes| {
            rmp_serde::from_slice::<StoredWFStateInputDeSer>(&bytes)
                .expect("Can decode wf stream input")
        })
        .filter_map(|si| {
            future::ready(match si {
                StoredWFStateInputDeSer::Stream(wfsi) => Some(wfsi),
                StoredWFStateInputDeSer::ImmediateLASinkResolutions(lares) => {
                    la_resp_q_clone.push(lares);
                    None
                }
            })
        });
    let basics = build_wf_basics(
        &mut config,
        MetricsContext::no_op(),
        CancellationToken::new(),
        DEFAULT_TEST_CAPABILITIES.clone(),
    );
    let sink = ReadingFromFileLaReqSink {
        resolutions: la_resp_q,
    };
    info!("Beginning workflow stream internal state replay");
    let stream = WFStream::build_internal(inputs, basics, sink);
    stream
        .for_each(|o| async move { trace!("Stream output: {:?}", o) })
        .await;
}

impl WFStream {
    pub(super) fn prep_input(&mut self, action: &WFStreamInput) -> Option<PreppedInputWrite> {
        // Remove the channel, we'll put it back, avoiding a clone
        self.wf_state_inputs.take().map(|chan| PreppedInputWrite {
            data: rmp_serde::to_vec(&StoredWFStateInputSer::Stream(action))
                .expect("WF Inputs are serializable"),
            chan,
        })
    }
    pub(super) fn flush_write(&mut self, w: PreppedInputWrite) {
        let _ = w.chan.send(w.data);
        self.wf_state_inputs = Some(w.chan);
    }
}
pub(super) struct PreppedInputWrite {
    data: Vec<u8>,
    chan: UnboundedSender<Vec<u8>>,
}

#[derive(Serialize)]
enum StoredWFStateInputSer<'a> {
    Stream(&'a WFStreamInput),
    ImmediateLASinkResolutions(&'a Vec<LocalActivityResolution>),
}

#[derive(Deserialize)]
enum StoredWFStateInputDeSer {
    Stream(WFStreamInput),
    ImmediateLASinkResolutions(Vec<LocalActivityResolution>),
}

struct ReadingFromFileLaReqSink {
    resolutions: Arc<SegQueue<Vec<LocalActivityResolution>>>,
}
impl LocalActivityRequestSink for ReadingFromFileLaReqSink {
    fn sink_reqs(&self, reqs: Vec<LocalActRequest>) -> Vec<LocalActivityResolution> {
        if !reqs.is_empty() {
            self.resolutions
                .pop()
                .expect("LA sink was called, but there's no stored immediate response")
        } else {
            vec![]
        }
    }
}

impl LAReqSink {
    pub(crate) fn write_req(&self, res: &Vec<LocalActivityResolution>) {
        if let Some(r) = self.recorder.as_ref() {
            r.send(
                rmp_serde::to_vec(&StoredWFStateInputSer::ImmediateLASinkResolutions(res))
                    .expect("LA immediate resolutions are serializable"),
            )
            .expect("WF input serialization channel is available for immediate LA result storage");
        }
    }
}
