use crate::{
    abstractions::OwnedMeteredSemPermit,
    protosext::ValidPollWFTQResponse,
    worker::{
        client::WorkerClient,
        workflow::{history_update::HistoryPaginator, HistoryFetchReq, PermittedWFT},
    },
};
use futures::Stream;
use futures_util::{future, stream, FutureExt, StreamExt};
use std::{collections::HashMap, sync::Arc, task::Poll};
use tokio_util::sync::CancellationToken;

/// Transforms incoming validated WFTs and history fetching requests into [PermittedWFT]s ready
/// for application to workflow state
pub(super) struct WFTExtractor {
    /// Maps run ids to their associated paginator
    paginators: HashMap<String, HistoryPaginator>,
}

enum WFTExtractorInput {
    New((HistoryPaginator, PermittedWFT)),
    FetchResult((HistoryPaginator, PermittedWFT)),
    FatalPollErr(tonic::Status),
    /// If paginating or history fetching fails, we don't want to consider that a fatal polling
    /// error
    FetchErr {
        run_id: String,
        err: tonic::Status,
    },
}

pub(super) enum WFTExtractorOutput {
    NewWFT(PermittedWFT),
    FetchResult(PermittedWFT),
    FailedFetch { run_id: String, err: tonic::Status },
}

type WFTStreamIn = (
    Result<ValidPollWFTQResponse, tonic::Status>,
    OwnedMeteredSemPermit,
);
impl WFTExtractor {
    pub(super) fn build(
        client: Arc<dyn WorkerClient>,
        wft_stream: impl Stream<Item = WFTStreamIn> + Send + 'static,
        fetch_stream: impl Stream<Item = HistoryFetchReq> + Send + 'static,
    ) -> impl Stream<Item = Result<WFTExtractorOutput, tonic::Status>> + Send + 'static {
        let extractor = Self {
            paginators: Default::default(),
        };
        let stop_tok = CancellationToken::new();
        let wft_stream_end_stopper = stop_tok.clone();
        let fetch_client = client.clone();
        let wft_stream = wft_stream
            .map(move |(wft, permit)| {
                let client = client.clone();
                async move {
                    match wft {
                        Ok(wft) => {
                            let prev_id = wft.previous_started_event_id;
                            let run_id = wft.workflow_execution.run_id.clone();
                            match HistoryPaginator::from_poll(wft, client, prev_id).await {
                                Ok((pag, prep)) => WFTExtractorInput::New((
                                    pag,
                                    PermittedWFT { work: prep, permit },
                                )),
                                Err(err) => WFTExtractorInput::FetchErr { run_id, err },
                            }
                        }
                        Err(e) => WFTExtractorInput::FatalPollErr(e),
                    }
                }
                .left_future()
            })
            .chain(stream::poll_fn(move |_| {
                wft_stream_end_stopper.cancel();
                Poll::Pending
            }));
        stream::select(
            wft_stream,
            fetch_stream.map(move |fetchreq| {
                let client = fetch_client.clone();
                let run_id = fetchreq.original_wft.work.execution.run_id.clone();
                async move {
                    match HistoryPaginator::from_fetchreq(fetchreq, client).await {
                        Ok(r) => WFTExtractorInput::FetchResult(r),
                        Err(err) => WFTExtractorInput::FetchErr { run_id, err },
                    }
                }
                .right_future()
            }),
        )
        // TODO: This will drop any in-progress cache misses or pagination.
        //   It would be fine to drop cache misses, but pagination requests should probably
        //   go through. Could attach
        //   Could send a poller shutdown item from stream, then keep polling
        //   fetch pipe until somehow closing send side after wf stream acks shutdown and flushes
        //   requests.
        .take_until(async move { stop_tok.cancelled().await })
        // TODO:  Configurable.
        .buffer_unordered(25)
        .scan(extractor, |ex, extinput| {
            future::ready(Some(match extinput {
                WFTExtractorInput::New((paginator, pwft)) => {
                    ex.paginators
                        .insert(pwft.work.execution.run_id.clone(), paginator);
                    Ok(WFTExtractorOutput::NewWFT(pwft))
                }
                WFTExtractorInput::FetchResult((paginator, pwft)) => {
                    ex.paginators
                        .insert(pwft.work.execution.run_id.clone(), paginator);
                    Ok(WFTExtractorOutput::FetchResult(pwft))
                }
                WFTExtractorInput::FatalPollErr(e) => Err(e),
                WFTExtractorInput::FetchErr { run_id, err } => {
                    Ok(WFTExtractorOutput::FailedFetch { run_id, err })
                }
            }))
        })
    }
}
