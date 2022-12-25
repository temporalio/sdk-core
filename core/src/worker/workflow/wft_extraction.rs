use crate::{
    protosext::ValidPollWFTQResponse,
    worker::{
        client::WorkerClient,
        workflow::{HistoryFetchReq, HistoryUpdate, PreparedWFT, WFTPaginator},
    },
};
use futures::Stream;
use futures_util::{future, stream, FutureExt, StreamExt};
use std::{collections::HashMap, sync::Arc, task::Poll};
use tokio_util::sync::CancellationToken;

/// Transforms incoming validated WFTs and history fetching requests into [PreparedWFT]s ready
/// for application to workflow state
pub(super) struct WFTExtractor {
    /// Maps run ids to their associated paginator
    paginators: HashMap<String, WFTPaginator>,
}

#[derive(derive_more::From)]
enum WFTExtractorInput {
    New(Result<(WFTPaginator, PreparedWFT), tonic::Status>),
    FetchReq(HistoryFetchReq),
}

pub(super) enum WFTExtractorOutput {
    NewWFT(PreparedWFT),
    FetchResult {
        run_id: String,
        update: HistoryUpdate,
    },
}

impl WFTExtractor {
    pub(super) fn new(
        client: Arc<dyn WorkerClient>,
        wft_stream: impl Stream<Item = Result<ValidPollWFTQResponse, tonic::Status>> + Send + 'static,
        fetch_stream: impl Stream<Item = HistoryFetchReq> + Send + 'static,
    ) -> impl Stream<Item = Result<WFTExtractorOutput, tonic::Status>> + Send + 'static {
        let extractor = Self {
            paginators: Default::default(),
        };
        let stop_tok = CancellationToken::new();
        let wft_stream_end_stopper = stop_tok.clone();
        let wft_stream = wft_stream
            .map(move |wft| {
                let client = client.clone();
                async move {
                    match wft {
                        Ok(wft) => {
                            // TODO: Get last processed ID somehow - not sure it can even work here
                            //  since it might change by the time it makes it in -- maybe just wait
                            //  to drop until actually applying.
                            WFTPaginator::from_poll(wft, client, 0).await.into()
                        }
                        Err(e) => Err(e).into(),
                    }
                }
                .left_future()
            })
            // We want the whole stream to end once the wft inputs die
            .chain(stream::poll_fn(move |_| {
                wft_stream_end_stopper.cancel();
                Poll::Pending
            }));
        stream::select(
            wft_stream,
            fetch_stream.map(|fetchreq| future::ready(fetchreq.into()).right_future()),
        )
        .take_until(async move { stop_tok.cancelled().await })
        // TODO:  Configurable.
        .buffer_unordered(25)
        .scan(extractor, |ex, extinput| {
            match extinput {
                WFTExtractorInput::New(Ok((paginator, pwft))) => {
                    ex.paginators
                        .insert(pwft.execution.run_id.clone(), paginator);
                    future::ready(Some(Ok(WFTExtractorOutput::NewWFT(pwft)))).left_future()
                }
                WFTExtractorInput::New(Err(e)) => future::ready(Some(Err(e))).left_future(),
                WFTExtractorInput::FetchReq(req) => {
                    if let Some(mut pg) = ex.paginators.remove(&req.run_id) {
                        async move {
                            Some(
                                // TODO: Pass last processed id in fetchreq
                                match pg.paginator.extract_next_update(0).await {
                                    Ok(update) => Ok(WFTExtractorOutput::FetchResult {
                                        run_id: req.run_id,
                                        update,
                                    }),
                                    Err(e) => Err(e),
                                },
                            )
                        }
                        .right_future()
                    } else {
                        error!(run_id=%req.run_id, "WFT paginator not found");
                        todo!("SKip here");
                    }
                }
            }
        })
    }
}
