use crate::{
    abstractions::OwnedMeteredSemPermit,
    protosext::ValidPollWFTQResponse,
    worker::{
        client::WorkerClient,
        workflow::{
            history_update::HistoryPaginator, CacheMissFetchReq, HistoryUpdate, NextPageReq,
            PermittedWFT,
        },
    },
};
use futures::Stream;
use futures_util::{stream, stream::PollNext, FutureExt, StreamExt};
use std::{sync::Arc, task::Poll};
use tokio_util::sync::CancellationToken;
use tracing::Span;

/// Transforms incoming validated WFTs and history fetching requests into [PermittedWFT]s ready
/// for application to workflow state
pub(super) struct WFTExtractor {}

pub(super) enum WFTExtractorOutput {
    NewWFT(PermittedWFT),
    FetchResult(PermittedWFT),
    NextPage {
        paginator: HistoryPaginator,
        update: HistoryUpdate,
        span: Span,
    },
    FailedFetch {
        run_id: String,
        err: tonic::Status,
    },
}

type WFTStreamIn = (
    Result<ValidPollWFTQResponse, tonic::Status>,
    OwnedMeteredSemPermit,
);
#[derive(derive_more::From, Debug)]
pub(super) enum HistoryFetchReq {
    Full(CacheMissFetchReq),
    NextPage(NextPageReq),
}

impl WFTExtractor {
    pub(super) fn build(
        client: Arc<dyn WorkerClient>,
        wft_stream: impl Stream<Item = WFTStreamIn> + Send + 'static,
        fetch_stream: impl Stream<Item = HistoryFetchReq> + Send + 'static,
    ) -> impl Stream<Item = Result<WFTExtractorOutput, tonic::Status>> + Send + 'static {
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
                            Ok(
                                match HistoryPaginator::from_poll(wft, client, prev_id).await {
                                    Ok((pag, prep)) => WFTExtractorOutput::NewWFT(PermittedWFT {
                                        work: prep,
                                        permit,
                                        paginator: pag,
                                    }),
                                    Err(err) => WFTExtractorOutput::FailedFetch { run_id, err },
                                },
                            )
                        }
                        Err(e) => Err(e),
                    }
                }
                .left_future()
            })
            .chain(stream::poll_fn(move |_| {
                wft_stream_end_stopper.cancel();
                Poll::Pending
            }));

        stream::select_with_strategy(
            wft_stream,
            fetch_stream.map(move |fetchreq: HistoryFetchReq| {
                let client = fetch_client.clone();
                async move {
                    Ok(match fetchreq {
                        HistoryFetchReq::Full(req) => {
                            let run_id = req.original_wft.work.execution.run_id.clone();
                            match HistoryPaginator::from_fetchreq(req, client).await {
                                Ok(r) => WFTExtractorOutput::FetchResult(r),
                                Err(err) => WFTExtractorOutput::FailedFetch { run_id, err },
                            }
                        }
                        HistoryFetchReq::NextPage(mut req) => {
                            match req
                                .paginator
                                .extract_next_update(req.last_processed_id)
                                .await
                            {
                                Ok(update) => WFTExtractorOutput::NextPage {
                                    paginator: req.paginator,
                                    update,
                                    span: req.span,
                                },
                                Err(err) => WFTExtractorOutput::FailedFetch {
                                    run_id: req.paginator.run_id,
                                    err,
                                },
                            }
                        }
                    })
                }
                .right_future()
            }),
            // Priority always goes to the fetching stream
            |_: &mut ()| PollNext::Right,
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
    }
}
