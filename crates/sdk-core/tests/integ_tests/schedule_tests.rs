use crate::common::{NAMESPACE, eventually, get_integ_client, rand_6_chars};
use std::time::{Duration, SystemTime};
use temporalio_client::{
    CreateScheduleOptions, ListSchedulesOptions, ScheduleAction, ScheduleBackfill,
    ScheduleCalendarSpec, ScheduleIntervalSpec, ScheduleOverlapPolicy, ScheduleSpec,
};

async fn test_client() -> temporalio_client::Client {
    get_integ_client(NAMESPACE.to_string(), None).await
}

fn test_schedule_id(label: &str) -> String {
    format!("sched-{label}-{}", rand_6_chars())
}

fn simple_action() -> ScheduleAction {
    ScheduleAction::start_workflow(
        "NoSuchWorkflow",
        format!("sched-tq-{}", rand_6_chars()),
        format!("sched-wf-{}", rand_6_chars()),
    )
}

fn hourly_spec() -> ScheduleSpec {
    ScheduleSpec::builder()
        .intervals(vec![
            ScheduleIntervalSpec::builder()
                .every(Duration::from_secs(3600))
                .build(),
        ])
        .build()
}

fn paused_opts(note: &str) -> CreateScheduleOptions {
    CreateScheduleOptions::builder()
        .paused(true)
        .note(note)
        .build()
}

#[tokio::test]
async fn create_and_describe_schedule() {
    let client = test_client().await;
    let schedule_id = test_schedule_id("create-describe");

    let handle = client
        .create_schedule(
            &schedule_id,
            simple_action(),
            hourly_spec(),
            paused_opts("created paused for testing"),
        )
        .await
        .unwrap();

    assert_eq!(handle.schedule_id, schedule_id);

    let desc = handle.describe().await.unwrap();
    assert!(desc.paused());
    assert_eq!(desc.notes(), Some("created paused for testing"));
    assert!(!desc.conflict_token().is_empty());

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn create_schedule_with_calendar_spec() {
    let client = test_client().await;
    let schedule_id = test_schedule_id("calendar");

    let spec = ScheduleSpec::builder()
        .calendars(vec![ScheduleCalendarSpec::builder().hour("2-7").build()])
        .build();

    let handle = client
        .create_schedule(
            &schedule_id,
            simple_action(),
            spec,
            paused_opts("calendar test"),
        )
        .await
        .unwrap();

    let desc = handle.describe().await.unwrap();
    let raw_spec = desc.raw().schedule.as_ref().unwrap().spec.as_ref().unwrap();
    assert_eq!(raw_spec.structured_calendar.len(), 1);

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn pause_and_unpause_schedule() {
    let client = test_client().await;
    let schedule_id = test_schedule_id("pause-unpause");

    let handle = client
        .create_schedule(
            &schedule_id,
            simple_action(),
            hourly_spec(),
            CreateScheduleOptions::default(),
        )
        .await
        .unwrap();

    let desc = handle.describe().await.unwrap();
    assert!(!desc.paused());

    handle.pause("pausing for test").await.unwrap();
    let desc = handle.describe().await.unwrap();
    assert!(desc.paused());
    assert_eq!(desc.notes(), Some("pausing for test"));

    handle.unpause("resuming").await.unwrap();
    let desc = handle.describe().await.unwrap();
    assert!(!desc.paused());
    assert_eq!(desc.notes(), Some("resuming"));

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn update_schedule() {
    let client = test_client().await;
    let schedule_id = test_schedule_id("update");

    let handle = client
        .create_schedule(
            &schedule_id,
            simple_action(),
            hourly_spec(),
            paused_opts("before update"),
        )
        .await
        .unwrap();

    handle
        .update(|u| u.set_note("updated notes"))
        .await
        .unwrap();

    let desc = handle.describe().await.unwrap();
    assert_eq!(desc.notes(), Some("updated notes"));

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn trigger_schedule() {
    let client = test_client().await;
    let schedule_id = test_schedule_id("trigger");

    let handle = client
        .create_schedule(
            &schedule_id,
            simple_action(),
            hourly_spec(),
            paused_opts("trigger test"),
        )
        .await
        .unwrap();

    handle.trigger().await.unwrap();

    let desc = eventually(
        || {
            let handle = client.get_schedule_handle(&schedule_id);
            async move {
                let desc = handle.describe().await.map_err(|e| e.to_string())?;
                if desc.action_count() > 0 {
                    Ok(desc)
                } else {
                    Err("action_count still 0".to_string())
                }
            }
        },
        Duration::from_secs(10),
    )
    .await
    .unwrap();

    assert!(desc.action_count() >= 1);

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn backfill_schedule() {
    let client = test_client().await;
    let schedule_id = test_schedule_id("backfill");

    let handle = client
        .create_schedule(
            &schedule_id,
            simple_action(),
            hourly_spec(),
            paused_opts("backfill test"),
        )
        .await
        .unwrap();

    let now = SystemTime::now();
    let two_hours_ago = now - Duration::from_secs(7200);
    handle
        .backfill(vec![
            ScheduleBackfill::builder()
                .start_time(two_hours_ago)
                .end_time(now)
                .overlap_policy(ScheduleOverlapPolicy::AllowAll)
                .build(),
        ])
        .await
        .unwrap();

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn delete_schedule() {
    let client = test_client().await;
    let schedule_id = test_schedule_id("delete");

    let handle = client
        .create_schedule(
            &schedule_id,
            simple_action(),
            hourly_spec(),
            paused_opts("delete test"),
        )
        .await
        .unwrap();

    handle.delete().await.unwrap();

    let err = handle.describe().await.unwrap_err();
    assert!(err.to_string().contains("not found") || err.to_string().contains("NotFound"));
}

#[tokio::test]
async fn get_schedule_handle_for_existing_schedule() {
    let client = test_client().await;
    let schedule_id = test_schedule_id("get-handle");

    client
        .create_schedule(
            &schedule_id,
            simple_action(),
            hourly_spec(),
            paused_opts("get handle test"),
        )
        .await
        .unwrap();

    let handle = client.get_schedule_handle(&schedule_id);
    let desc = handle.describe().await.unwrap();
    assert!(desc.paused());

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn list_schedules() {
    let client = test_client().await;
    let suffix = rand_6_chars();
    let num_schedules = 3;
    let mut schedule_ids = Vec::new();

    for i in 0..num_schedules {
        let schedule_id = format!("sched-list-{suffix}-{i}");
        schedule_ids.push(schedule_id.clone());
        client
            .create_schedule(
                &schedule_id,
                simple_action(),
                hourly_spec(),
                paused_opts("list test"),
            )
            .await
            .unwrap();
    }

    let found_ids = eventually(
        || {
            let client = client.clone();
            let expected = schedule_ids.clone();
            async move {
                let page = client
                    .list_schedules(ListSchedulesOptions::default())
                    .await
                    .map_err(|e| e.to_string())?;
                let found: Vec<_> = page
                    .schedules
                    .iter()
                    .filter(|s| expected.contains(&s.schedule_id().to_string()))
                    .map(|s| s.schedule_id().to_string())
                    .collect();
                if found.len() == expected.len() {
                    Ok(found)
                } else {
                    Err(format!(
                        "Expected {} schedules, found {}",
                        expected.len(),
                        found.len()
                    ))
                }
            }
        },
        Duration::from_secs(10),
    )
    .await
    .unwrap();

    assert_eq!(found_ids.len(), num_schedules);

    for id in &schedule_ids {
        client.get_schedule_handle(id).delete().await.unwrap();
    }
}

#[tokio::test]
async fn describe_accessors_match_created_values() {
    let client = test_client().await;
    let schedule_id = test_schedule_id("accessors");

    let handle = client
        .create_schedule(
            &schedule_id,
            simple_action(),
            hourly_spec(),
            paused_opts("accessors test"),
        )
        .await
        .unwrap();

    let desc = handle.describe().await.unwrap();

    assert!(desc.paused());
    assert_eq!(desc.notes(), Some("accessors test"));
    assert_eq!(desc.action_count(), 0);
    assert_eq!(desc.missed_catchup_window(), 0);
    assert_eq!(desc.overlap_skipped(), 0);
    assert!(desc.recent_actions().is_empty());
    assert!(desc.running_actions().is_empty());
    assert!(desc.create_time().is_some());

    handle.delete().await.unwrap();
}
