use crate::common::*;
use futures_util::StreamExt;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use temporalio_client::WorkflowOptions;
use temporalio_common::protos::{
    DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder,
    temporal::api::{
        common::v1::WorkflowExecution,
        enums::v1::{EventType, WorkflowTaskFailedCause},
        history::v1::{History, HistoryEvent},
        workflowservice::v1::GetWorkflowExecutionHistoryResponse,
    },
};
use temporalio_sdk::WfContext;
use temporalio_sdk_core::test_help::{MockPollCfg, ResponseType, mock_worker_client};

#[tokio::test]
async fn weird_pagination_doesnt_drop_wft_events() {
    let wf_id = "fakeid";
    // 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
    // 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    // 3: EVENT_TYPE_WORKFLOW_TASK_STARTED
    // 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    // empty page
    // 5: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
    // 6: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    // 7: EVENT_TYPE_WORKFLOW_TASK_STARTED
    // 8: EVENT_TYPE_WORKFLOW_TASK_FAILED
    // empty page
    // 9: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
    // 10: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    // 11: EVENT_TYPE_WORKFLOW_TASK_STARTED
    // empty page
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();

    t.add_we_signaled("hi", vec![]);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::UnhandledCommand,
        Default::default(),
    );

    t.add_we_signaled("hi", vec![]);
    t.add_workflow_task_scheduled_and_started();

    let workflow_task = t.get_full_history_info().unwrap();
    let mut wft_resp = workflow_task.as_poll_wft_response();
    wft_resp.workflow_execution = Some(WorkflowExecution {
        workflow_id: wf_id.to_string(),
        run_id: t.get_orig_run_id().to_string(),
    });
    // Just 9/10/11 in WFT
    wft_resp.history.as_mut().unwrap().events.drain(0..8);

    let mut resp_1: GetWorkflowExecutionHistoryResponse = t.get_full_history_info().unwrap().into();
    resp_1.next_page_token = vec![1];
    resp_1.history.as_mut().unwrap().events.truncate(4);

    let mut mock_client = mock_worker_client();
    mock_client
        .expect_get_workflow_execution_history()
        .returning(move |_, _, _| Ok(resp_1.clone()))
        .times(1);
    mock_client
        .expect_get_workflow_execution_history()
        .returning(move |_, _, _| {
            Ok(GetWorkflowExecutionHistoryResponse {
                history: Some(History { events: vec![] }),
                raw_history: vec![],
                next_page_token: vec![2],
                archived: false,
            })
        })
        .times(1);
    let mut resp_2: GetWorkflowExecutionHistoryResponse = t.get_full_history_info().unwrap().into();
    resp_2.next_page_token = vec![3];
    resp_2.history.as_mut().unwrap().events.drain(0..4);
    resp_2.history.as_mut().unwrap().events.truncate(4);
    mock_client
        .expect_get_workflow_execution_history()
        .returning(move |_, _, _| Ok(resp_2.clone()))
        .times(1);
    mock_client
        .expect_get_workflow_execution_history()
        .returning(move |_, _, _| {
            Ok(GetWorkflowExecutionHistoryResponse {
                history: Some(History { events: vec![] }),
                raw_history: vec![],
                next_page_token: vec![],
                archived: false,
            })
        })
        .times(1);

    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [ResponseType::Raw(wft_resp)], mock_client);
    let mut worker = mock_sdk_cfg(mh, |cfg| {
        cfg.max_cached_workflows = 2;
        cfg.ignore_evicts_on_shutdown = false;
    });

    let sig_ctr = Arc::new(AtomicUsize::new(0));
    let sig_ctr_clone = sig_ctr.clone();
    worker.register_wf(wf_type.to_owned(), move |ctx: WfContext| {
        let sig_ctr_clone = sig_ctr_clone.clone();
        async move {
            let mut sigchan = ctx.make_signal_channel("hi");
            while sigchan.next().await.is_some() {
                if sig_ctr_clone.fetch_add(1, Ordering::AcqRel) == 1 {
                    break;
                }
            }
            Ok(().into())
        }
    });

    worker
        .submit_wf(
            wf_id.to_owned(),
            wf_type.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    assert_eq!(sig_ctr.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn extreme_pagination_doesnt_drop_wft_events_worker() {
    let wf_id = "fakeid";

    // In this test, we add empty pages between each event

    // 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
    // 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    // 3: EVENT_TYPE_WORKFLOW_TASK_STARTED // <- previous_started_event_id
    // 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED

    // 5: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
    // 6: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    // 7: EVENT_TYPE_WORKFLOW_TASK_STARTED
    // 8: EVENT_TYPE_WORKFLOW_TASK_FAILED

    // 9: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
    // 10: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
    // 11: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
    // 12: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
    // 13: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
    // 14: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    // 15: EVENT_TYPE_WORKFLOW_TASK_STARTED // <- started_event_id

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();

    t.add_we_signaled("hi", vec![]);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::UnhandledCommand,
        Default::default(),
    );

    t.add_we_signaled("hi", vec![]);
    t.add_we_signaled("hi", vec![]);
    t.add_we_signaled("hi", vec![]);
    t.add_we_signaled("hi", vec![]);
    t.add_we_signaled("hi", vec![]);
    t.add_workflow_task_scheduled_and_started();

    /////

    let events: Vec<HistoryEvent> = t.get_full_history_info().unwrap().into_events();
    let first_event = events[0].clone();

    let mut mock_client = mock_worker_client();

    for (i, event) in events.into_iter().enumerate() {
        // Add an empty page
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History { events: vec![] }),
                    raw_history: vec![],
                    next_page_token: vec![(i * 10 + 1) as u8],
                    archived: false,
                })
            })
            .times(1);

        // Add a page with just event i
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History {
                        events: vec![event.clone()],
                    }),
                    raw_history: vec![],
                    next_page_token: vec![(i * 10) as u8],
                    archived: false,
                })
            })
            .times(1);
    }

    // Add an extra empty page at the end, with no NPT
    mock_client
        .expect_get_workflow_execution_history()
        .returning(move |_, _, _| {
            Ok(GetWorkflowExecutionHistoryResponse {
                history: Some(History { events: vec![] }),
                raw_history: vec![],
                next_page_token: vec![],
                archived: false,
            })
        })
        .times(1);

    let workflow_task = t.get_full_history_info().unwrap();
    let mut wft_resp = workflow_task.as_poll_wft_response();
    wft_resp.workflow_execution = Some(WorkflowExecution {
        workflow_id: wf_id.to_string(),
        run_id: t.get_orig_run_id().to_string(),
    });
    wft_resp.history = Some(History {
        events: vec![first_event],
    });
    wft_resp.next_page_token = vec![1];
    wft_resp.previous_started_event_id = 3;
    wft_resp.started_event_id = 15;

    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [ResponseType::Raw(wft_resp)], mock_client);
    let mut worker = mock_sdk_cfg(mh, |cfg| {
        cfg.max_cached_workflows = 2;
        cfg.ignore_evicts_on_shutdown = false;
    });

    let sig_ctr = Arc::new(AtomicUsize::new(0));
    let sig_ctr_clone = sig_ctr.clone();
    worker.register_wf(wf_type.to_owned(), move |ctx: WfContext| {
        let sig_ctr_clone = sig_ctr_clone.clone();
        async move {
            let mut sigchan = ctx.make_signal_channel("hi");
            while sigchan.next().await.is_some() {
                if sig_ctr_clone.fetch_add(1, Ordering::AcqRel) == 5 {
                    break;
                }
            }
            Ok(().into())
        }
    });

    worker
        .submit_wf(
            wf_id.to_owned(),
            wf_type.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    assert_eq!(sig_ctr.load(Ordering::Acquire), 6);
}
