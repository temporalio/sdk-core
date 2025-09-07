use crate::common::{ActivationAssertionsInterceptor, CoreWfStarter, build_fake_sdk};
use std::{
    collections::{HashSet, VecDeque, hash_map::RandomState},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use temporal_client::WorkflowClientTrait;
use temporal_sdk::{ActivityOptions, WfContext, WorkflowResult};
use temporal_sdk_core::test_help::{CoreInternalFlags, MockPollCfg, ResponseType};
use temporal_sdk_core_protos::{
    DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder, VERSION_SEARCH_ATTR_KEY,
    constants::PATCH_MARKER_NAME,
    coresdk::{
        AsJsonPayloadExt, FromJsonPayloadExt,
        common::decode_change_marker_details,
        workflow_activation::{NotifyHasPatch, WorkflowActivationJob, workflow_activation_job},
    },
    temporal::api::{
        command::v1::{
            RecordMarkerCommandAttributes, ScheduleActivityTaskCommandAttributes,
            UpsertWorkflowSearchAttributesCommandAttributes, command::Attributes,
        },
        common::v1::ActivityType,
        enums::v1::{CommandType, EventType},
        history::v1::{
            ActivityTaskCompletedEventAttributes, ActivityTaskScheduledEventAttributes,
            ActivityTaskStartedEventAttributes, TimerFiredEventAttributes,
        },
    },
};
use tokio::{join, sync::Notify};
use tokio_stream::StreamExt;

const MY_PATCH_ID: &str = "integ_test_change_name";

pub(crate) async fn changes_wf(ctx: WfContext) -> WorkflowResult<()> {
    if ctx.patched(MY_PATCH_ID) {
        ctx.timer(Duration::from_millis(100)).await;
    } else {
        ctx.timer(Duration::from_millis(200)).await;
    }
    ctx.timer(Duration::from_millis(200)).await;
    if ctx.patched(MY_PATCH_ID) {
        ctx.timer(Duration::from_millis(100)).await;
    } else {
        ctx.timer(Duration::from_millis(200)).await;
    }
    Ok(().into())
}

#[tokio::test]
async fn writes_change_markers() {
    let wf_name = "writes_change_markers";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), changes_wf);

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

/// This one simulates a run as if the worker had the "old" code, then it fails at the end as
/// a cheapo way of being re-run, at which point it runs with change checks and the "new" code.
static DID_DIE: AtomicBool = AtomicBool::new(false);

pub(crate) async fn no_change_then_change_wf(ctx: WfContext) -> WorkflowResult<()> {
    if DID_DIE.load(Ordering::Acquire) {
        assert!(!ctx.patched(MY_PATCH_ID));
    }
    ctx.timer(Duration::from_millis(200)).await;
    ctx.timer(Duration::from_millis(200)).await;
    if DID_DIE.load(Ordering::Acquire) {
        assert!(!ctx.patched(MY_PATCH_ID));
    }
    ctx.timer(Duration::from_millis(200)).await;

    if !DID_DIE.load(Ordering::Acquire) {
        DID_DIE.store(true, Ordering::Release);
        ctx.force_task_fail(anyhow::anyhow!("i'm ded"));
    }
    Ok(().into())
}

#[tokio::test]
async fn can_add_change_markers() {
    let wf_name = "can_add_change_markers";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), no_change_then_change_wf);

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

static DID_DIE_2: AtomicBool = AtomicBool::new(false);

pub(crate) async fn replay_with_change_marker_wf(ctx: WfContext) -> WorkflowResult<()> {
    assert!(ctx.patched(MY_PATCH_ID));
    ctx.timer(Duration::from_millis(200)).await;
    if !DID_DIE_2.load(Ordering::Acquire) {
        DID_DIE_2.store(true, Ordering::Release);
        ctx.force_task_fail(anyhow::anyhow!("i'm ded"));
    }
    Ok(().into())
}

#[tokio::test]
async fn replaying_with_patch_marker() {
    let wf_name = "replaying_with_patch_marker";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), replay_with_change_marker_wf);

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

/// Test that the internal patching mechanism works on the second workflow task when replaying.
/// Used as regression test for a bug that detected that we did not look ahead far enough to find
/// the next workflow task completion, which the flags are attached to.
#[tokio::test]
async fn patched_on_second_workflow_task_is_deterministic() {
    let wf_name = "timer_patched_timer";
    let mut starter = CoreWfStarter::new(wf_name);
    // Disable caching to force replay from beginning
    starter
        .worker_config
        .max_cached_workflows(0_usize)
        .no_remote_activities(true);
    let mut worker = starter.worker().await;
    // Include a task failure as well to make sure that works
    static FAIL_ONCE: AtomicBool = AtomicBool::new(true);
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        ctx.timer(Duration::from_millis(1)).await;
        if FAIL_ONCE.load(Ordering::Acquire) {
            FAIL_ONCE.store(false, Ordering::Release);
            panic!("Enchi is hungry!");
        }
        assert!(ctx.patched(MY_PATCH_ID));
        ctx.timer(Duration::from_millis(1)).await;
        Ok(().into())
    });

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn can_remove_deprecated_patch_near_other_patch() {
    let wf_name = "can_add_change_markers";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    let did_die = Arc::new(AtomicBool::new(false));
    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| {
        let did_die = did_die.clone();
        async move {
            ctx.timer(Duration::from_millis(200)).await;
            if !did_die.load(Ordering::Acquire) {
                assert!(ctx.deprecate_patch("getting-deprecated"));
                assert!(ctx.patched("staying"));
            } else {
                assert!(ctx.patched("staying"));
            }
            ctx.timer(Duration::from_millis(200)).await;

            if !did_die.load(Ordering::Acquire) {
                did_die.store(true, Ordering::Release);
                ctx.force_task_fail(anyhow::anyhow!("i'm ded"));
            }
            Ok(().into())
        }
    });

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn deprecated_patch_removal() {
    let wf_name = "deprecated_patch_removal";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    let wf_id = starter.get_task_queue().to_string();
    let did_die = Arc::new(AtomicBool::new(false));
    let send_sig = Arc::new(Notify::new());
    let send_sig_c = send_sig.clone();
    worker.register_wf(wf_name, move |ctx: WfContext| {
        let did_die = did_die.clone();
        let send_sig_c = send_sig_c.clone();
        async move {
            if !did_die.load(Ordering::Acquire) {
                assert!(ctx.deprecate_patch("getting-deprecated"));
            }
            send_sig_c.notify_one();
            ctx.make_signal_channel("sig").next().await;

            ctx.timer(Duration::from_millis(1)).await;

            if !did_die.load(Ordering::Acquire) {
                did_die.store(true, Ordering::Release);
                ctx.force_task_fail(anyhow::anyhow!("i'm ded"));
            }
            Ok(().into())
        }
    });

    starter.start_with_worker(wf_name, &mut worker).await;
    let sig_fut = async {
        send_sig.notified().await;
        client
            .signal_workflow_execution(wf_id, "".to_string(), "sig".to_string(), None, None)
            .await
            .unwrap()
    };
    let run_fut = async {
        worker.run_until_done().await.unwrap();
    };
    join!(sig_fut, run_fut);
}

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
enum MarkerType {
    Deprecated,
    NotDeprecated,
    NoMarker,
}

const ONE_SECOND: Duration = Duration::from_secs(1);

/// EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// EVENT_TYPE_WORKFLOW_TASK_STARTED
/// EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// EVENT_TYPE_MARKER_RECORDED (depending on marker_type)
/// EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
/// EVENT_TYPE_ACTIVITY_TASK_STARTED
/// EVENT_TYPE_ACTIVITY_TASK_COMPLETED
/// EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// EVENT_TYPE_WORKFLOW_TASK_STARTED
/// EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
fn patch_marker_single_activity(
    marker_type: MarkerType,
    version: usize,
    replay: bool,
) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.set_flags_first_wft(
        &[CoreInternalFlags::UpsertSearchAttributeOnPatch as u32],
        &[],
    );
    match marker_type {
        MarkerType::Deprecated => {
            t.add_has_change_marker(MY_PATCH_ID, true);
            t.add_upsert_search_attrs_for_patch(&[MY_PATCH_ID.to_string()]);
        }
        MarkerType::NotDeprecated => {
            t.add_has_change_marker(MY_PATCH_ID, false);
            t.add_upsert_search_attrs_for_patch(&[MY_PATCH_ID.to_string()]);
        }
        MarkerType::NoMarker => {}
    };

    let activity_id = if replay {
        match (marker_type, version) {
            (_, 1) => "no_change",
            (MarkerType::NotDeprecated, 2) => "had_change",
            (MarkerType::Deprecated, 2) => "had_change",
            (MarkerType::NoMarker, 2) => "no_change",
            (_, 3) => "had_change",
            (_, 4) => "had_change",
            v => panic!("Nonsense marker / version combo {v:?}"),
        }
    } else {
        // If the workflow isn't replaying (we're creating history here for a workflow which
        // wasn't replaying at the time of scheduling the activity, and has done that, and now
        // we're feeding back the history it would have produced) then it always has the change,
        // except in v1.
        if version > 1 {
            "had_change"
        } else {
            "no_change"
        }
    };

    let scheduled_event_id = t.add(ActivityTaskScheduledEventAttributes {
        activity_id: activity_id.to_string(),
        ..Default::default()
    });
    let started_event_id = t.add(ActivityTaskStartedEventAttributes {
        scheduled_event_id,
        ..Default::default()
    });
    t.add(ActivityTaskCompletedEventAttributes {
        scheduled_event_id,
        started_event_id,
        ..Default::default()
    });
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t
}

async fn v1(ctx: &mut WfContext) {
    ctx.activity(ActivityOptions {
        activity_id: Some("no_change".to_owned()),
        ..Default::default()
    })
    .await;
}

async fn v2(ctx: &mut WfContext) -> bool {
    if ctx.patched(MY_PATCH_ID) {
        ctx.activity(ActivityOptions {
            activity_id: Some("had_change".to_owned()),
            ..Default::default()
        })
        .await;
        true
    } else {
        ctx.activity(ActivityOptions {
            activity_id: Some("no_change".to_owned()),
            ..Default::default()
        })
        .await;
        false
    }
}

async fn v3(ctx: &mut WfContext) {
    ctx.deprecate_patch(MY_PATCH_ID);
    ctx.activity(ActivityOptions {
        activity_id: Some("had_change".to_owned()),
        ..Default::default()
    })
    .await;
}

async fn v4(ctx: &mut WfContext) {
    ctx.activity(ActivityOptions {
        activity_id: Some("had_change".to_owned()),
        ..Default::default()
    })
    .await;
}

fn patch_setup(replaying: bool, marker_type: MarkerType, workflow_version: usize) -> MockPollCfg {
    let t = patch_marker_single_activity(marker_type, workflow_version, replaying);
    if replaying {
        MockPollCfg::from_resps(t, [ResponseType::AllHistory])
    } else {
        MockPollCfg::from_hist_builder(t)
    }
}

macro_rules! patch_wf {
    ($workflow_version:ident) => {
        move |mut ctx: WfContext| async move {
            match $workflow_version {
                1 => {
                    v1(&mut ctx).await;
                }
                2 => {
                    v2(&mut ctx).await;
                }
                3 => {
                    v3(&mut ctx).await;
                }
                4 => {
                    v4(&mut ctx).await;
                }
                _ => panic!("Invalid workflow version for test setup"),
            }
            Ok(().into())
        }
    };
}

#[rstest]
#[case::v1_breaks_on_normal_marker(false, MarkerType::NotDeprecated, 1)]
#[case::v1_accepts_dep_marker(false, MarkerType::Deprecated, 1)]
#[case::v1_replay_breaks_on_normal_marker(true, MarkerType::NotDeprecated, 1)]
#[case::v1_replay_accepts_dep_marker(true, MarkerType::Deprecated, 1)]
#[case::v4_breaks_on_normal_marker(false, MarkerType::NotDeprecated, 4)]
#[case::v4_accepts_dep_marker(false, MarkerType::Deprecated, 4)]
#[case::v4_replay_breaks_on_normal_marker(true, MarkerType::NotDeprecated, 4)]
#[case::v4_replay_accepts_dep_marker(true, MarkerType::Deprecated, 4)]
#[tokio::test]
async fn v1_and_v4_changes(
    #[case] replaying: bool,
    #[case] marker_type: MarkerType,
    #[case] wf_version: usize,
) {
    let mut mock_cfg = patch_setup(replaying, marker_type, wf_version);

    if marker_type != MarkerType::Deprecated {
        // should explode b/c non-dep marker is present
        mock_cfg.num_expected_fails = 1;
    }

    let mut aai = ActivationAssertionsInterceptor::default();
    aai.skip_one().then(move |a| {
        if marker_type == MarkerType::Deprecated {
            // Activity is resolved
            assert_matches!(
                a.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(_))
                }]
            );
        }
    });

    if !replaying {
        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts.then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type,
                    CommandType::ScheduleActivityTask as i32
                );
            });
        });
    }

    let mut worker = build_fake_sdk(mock_cfg);
    worker.set_worker_interceptor(aai);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, patch_wf!(wf_version));
    worker.run().await.unwrap();
}

// Note that the not-replaying and no-marker cases don't make sense and hence are absent
#[rstest]
#[case::v2_marker_new_path(false, MarkerType::NotDeprecated, 2)]
#[case::v2_dep_marker_new_path(false, MarkerType::Deprecated, 2)]
#[case::v2_replay_no_marker_old_path(true, MarkerType::NoMarker, 2)]
#[case::v2_replay_marker_new_path(true, MarkerType::NotDeprecated, 2)]
#[case::v2_replay_dep_marker_new_path(true, MarkerType::Deprecated, 2)]
#[case::v3_marker_new_path(false, MarkerType::NotDeprecated, 3)]
#[case::v3_dep_marker_new_path(false, MarkerType::Deprecated, 3)]
#[case::v3_replay_no_marker_old_path(true, MarkerType::NoMarker, 3)]
#[case::v3_replay_marker_new_path(true, MarkerType::NotDeprecated, 3)]
#[case::v3_replay_dep_marker_new_path(true, MarkerType::Deprecated, 3)]
#[tokio::test]
async fn v2_and_v3_changes(
    #[case] replaying: bool,
    #[case] marker_type: MarkerType,
    #[case] wf_version: usize,
) {
    let mut mock_cfg = patch_setup(replaying, marker_type, wf_version);

    let mut aai = ActivationAssertionsInterceptor::default();
    aai.then(move |act| {
        // replaying cases should immediately get a resolve change activation when marker is
        // present
        if replaying && marker_type != MarkerType::NoMarker {
            assert_matches!(
                &act.jobs[1],
                 WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::NotifyHasPatch(
                        NotifyHasPatch {
                            patch_id,
                        }
                    ))
                } => patch_id == MY_PATCH_ID
            );
        } else {
            assert_eq!(act.jobs.len(), 1);
        }
    })
    .then(move |act| {
        assert_matches!(
            act.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_))
            }]
        );
    });

    if !replaying {
        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts.then(move |wft| {
                let mut commands = VecDeque::from(wft.commands.clone());
                let expected_num_cmds = if marker_type == MarkerType::NoMarker {
                    2
                } else {
                    3
                };
                assert_eq!(commands.len(), expected_num_cmds);
                let dep_flag_expected = wf_version != 2;
                assert_matches!(
                    commands.pop_front().unwrap().attributes.as_ref().unwrap(),
                    Attributes::RecordMarkerCommandAttributes(
                        RecordMarkerCommandAttributes { marker_name, details,.. })
                    if marker_name == PATCH_MARKER_NAME
                      && decode_change_marker_details(details).unwrap().1 == dep_flag_expected
                );
                if expected_num_cmds == 3 {
                    assert_matches!(
                        commands.pop_front().unwrap().attributes.as_ref().unwrap(),
                        Attributes::UpsertWorkflowSearchAttributesCommandAttributes(
                            UpsertWorkflowSearchAttributesCommandAttributes
                            { search_attributes: Some(attrs) }
                        )
                        if attrs.indexed_fields.get(VERSION_SEARCH_ATTR_KEY).unwrap()
                          == &[MY_PATCH_ID].as_json_payload().unwrap()
                    );
                }
                // The only time the "old" timer should fire is in v2, replaying, without a marker.
                let expected_activity_id =
                    if replaying && marker_type == MarkerType::NoMarker && wf_version == 2 {
                        "no_change"
                    } else {
                        "had_change"
                    };
                assert_matches!(
                    commands.pop_front().unwrap().attributes.as_ref().unwrap(),
                    Attributes::ScheduleActivityTaskCommandAttributes(
                        ScheduleActivityTaskCommandAttributes { activity_id, .. }
                    )
                    if activity_id == expected_activity_id
                );
            });
        });
    }

    let mut worker = build_fake_sdk(mock_cfg);
    worker.set_worker_interceptor(aai);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, patch_wf!(wf_version));
    worker.run().await.unwrap();
}

#[rstest]
#[case::has_change_replay(true, true)]
#[case::no_change_replay(false, true)]
#[case::has_change_inc(true, false)]
// The false-false case doesn't make sense, as the incremental cases act as if working against
// a sticky queue, and it'd be impossible for a worker with the call to get an incremental
// history that then suddenly doesn't have the marker.
#[tokio::test]
async fn same_change_multiple_spots(#[case] have_marker_in_hist: bool, #[case] replay: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.set_flags_first_wft(
        &[CoreInternalFlags::UpsertSearchAttributeOnPatch as u32],
        &[],
    );
    if have_marker_in_hist {
        t.add_has_change_marker(MY_PATCH_ID, false);
        t.add_upsert_search_attrs_for_patch(&[MY_PATCH_ID.to_string()]);
        let scheduled_event_id = t.add(ActivityTaskScheduledEventAttributes {
            activity_id: "1".to_owned(),
            activity_type: Some(ActivityType {
                name: "".to_string(),
            }),
            ..Default::default()
        });
        let started_event_id = t.add(ActivityTaskStartedEventAttributes {
            scheduled_event_id,
            ..Default::default()
        });
        t.add(ActivityTaskCompletedEventAttributes {
            scheduled_event_id,
            started_event_id,
            ..Default::default()
        });
        t.add_full_wf_task();
        let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
        t.add(TimerFiredEventAttributes {
            started_event_id: timer_started_event_id,
            timer_id: "1".to_owned(),
        });
    } else {
        let started_event_id = t.add_by_type(EventType::TimerStarted);
        t.add(TimerFiredEventAttributes {
            started_event_id,
            timer_id: "1".to_owned(),
        });
        t.add_full_wf_task();
        let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
        t.add(TimerFiredEventAttributes {
            started_event_id: timer_started_event_id,
            timer_id: "2".to_owned(),
        });
    }
    t.add_full_wf_task();

    if have_marker_in_hist {
        let scheduled_event_id = t.add(ActivityTaskScheduledEventAttributes {
            activity_id: "2".to_string(),
            activity_type: Some(ActivityType {
                name: "".to_string(),
            }),
            ..Default::default()
        });
        let started_event_id = t.add(ActivityTaskStartedEventAttributes {
            scheduled_event_id,
            ..Default::default()
        });
        t.add(ActivityTaskCompletedEventAttributes {
            scheduled_event_id,
            started_event_id,
            ..Default::default()
        });
    } else {
        let started_event_id = t.add_by_type(EventType::TimerStarted);
        t.add(TimerFiredEventAttributes {
            started_event_id,
            timer_id: "3".to_owned(),
        });
    }
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mock_cfg = if replay {
        MockPollCfg::from_resps(t, [ResponseType::AllHistory])
    } else {
        MockPollCfg::from_hist_builder(t)
    };

    // Errors would appear as nondeterminism problems, so just run it.
    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, move |ctx: WfContext| async move {
        if ctx.patched(MY_PATCH_ID) {
            ctx.activity(ActivityOptions::default()).await;
        } else {
            ctx.timer(ONE_SECOND).await;
        }
        ctx.timer(ONE_SECOND).await;
        if ctx.patched(MY_PATCH_ID) {
            ctx.activity(ActivityOptions::default()).await;
        } else {
            ctx.timer(ONE_SECOND).await;
        }
        Ok(().into())
    });
    worker.run().await.unwrap();
}

const SIZE_OVERFLOW_PATCH_AMOUNT: usize = 180;
#[rstest]
#[case::happy_path(50)]
// We start exceeding the 2k size limit at 180 patches with this format
#[case::size_overflow(SIZE_OVERFLOW_PATCH_AMOUNT)]
#[tokio::test]
async fn many_patches_combine_in_search_attrib_update(#[case] num_patches: usize) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.set_flags_first_wft(
        &[CoreInternalFlags::UpsertSearchAttributeOnPatch as u32],
        &[],
    );
    for i in 1..=num_patches {
        let id = format!("patch-{i}");
        t.add_has_change_marker(&id, false);
        if i < SIZE_OVERFLOW_PATCH_AMOUNT {
            t.add_upsert_search_attrs_for_patch(&[id]);
        }
        let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
        t.add(TimerFiredEventAttributes {
            started_event_id: timer_started_event_id,
            timer_id: i.to_string(),
        });
        t.add_full_wf_task();
    }
    t.add_workflow_execution_completed();

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        // Iterate through all activations/responses except the final one with complete workflow
        for i in 2..=num_patches + 1 {
            asserts.then(move |wft| {
                let cmds = &wft.commands;
                if i > SIZE_OVERFLOW_PATCH_AMOUNT {
                    assert_eq!(2, cmds.len());
                    assert_matches!(cmds[1].command_type(), CommandType::StartTimer);
                } else {
                    assert_eq!(3, cmds.len());
                    let attrs = assert_matches!(
                        cmds[1].attributes.as_ref().unwrap(),
                        Attributes::UpsertWorkflowSearchAttributesCommandAttributes(
                            UpsertWorkflowSearchAttributesCommandAttributes
                            { search_attributes: Some(attrs) }
                        ) => attrs
                    );
                    let expected_patches: HashSet<String, _> =
                        (1..i).map(|i| format!("patch-{i}")).collect();
                    let deserialized = HashSet::<String, RandomState>::from_json_payload(
                        attrs.indexed_fields.get(VERSION_SEARCH_ATTR_KEY).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(deserialized, expected_patches);
                }
            });
        }
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, move |ctx: WfContext| async move {
        for i in 1..=num_patches {
            let _dontcare = ctx.patched(&format!("patch-{i}"));
            ctx.timer(ONE_SECOND).await;
        }
        Ok(().into())
    });
    worker.run().await.unwrap();
}
