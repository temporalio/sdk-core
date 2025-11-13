use temporalio_common::worker::{WorkerConfigBuilder, WorkerTaskTypes, WorkerVersioningStrategy};

fn default_versioning_strategy() -> WorkerVersioningStrategy {
    WorkerVersioningStrategy::None {
        build_id: String::new(),
    }
}

#[test]
fn test_default_configuration_polls_all_types() {
    let config = WorkerConfigBuilder::default()
        .namespace("default")
        .task_queue("test-queue")
        .versioning_strategy(default_versioning_strategy())
        .build()
        .expect("Failed to build default config");

    let effective = config.task_types;
    assert!(
        effective.polls_workflows(),
        "Should poll workflows by default"
    );
    assert!(
        effective.polls_activities(),
        "Should poll activities by default"
    );
    assert!(effective.polls_nexus(), "Should poll nexus by default");
}

#[test]
fn test_workflow_only_worker() {
    let config = WorkerConfigBuilder::default()
        .namespace("default")
        .task_queue("test-queue")
        .versioning_strategy(default_versioning_strategy())
        .task_types(WorkerTaskTypes::WORKFLOWS)
        .max_cached_workflows(0usize)
        .build()
        .expect("Failed to build workflow-only config");

    let effective = config.task_types;
    assert!(effective.polls_workflows(), "Should poll workflows");
    assert!(!effective.polls_activities(), "Should NOT poll activities");
    assert!(!effective.polls_nexus(), "Should NOT poll nexus");
}

#[test]
fn test_activity_and_nexus_worker() {
    let config = WorkerConfigBuilder::default()
        .namespace("default")
        .task_queue("test-queue")
        .versioning_strategy(default_versioning_strategy())
        .task_types(WorkerTaskTypes::ACTIVITIES | WorkerTaskTypes::NEXUS)
        .max_cached_workflows(0usize)
        .build()
        .expect("Failed to build activity+nexus config");

    let effective = config.task_types;
    assert!(!effective.polls_workflows(), "Should NOT poll workflows");
    assert!(effective.polls_activities(), "Should poll activities");
    assert!(effective.polls_nexus(), "Should poll nexus");
}

#[test]
fn test_empty_task_types_fails_validation() {
    let result = WorkerConfigBuilder::default()
        .namespace("default")
        .task_queue("test-queue")
        .versioning_strategy(default_versioning_strategy())
        .task_types(WorkerTaskTypes::empty())
        .build();

    assert!(result.is_err(), "Empty task_types should fail validation");
    let err = result.err().unwrap().to_string();
    assert!(
        err.contains("At least one task type"),
        "Error should mention task types: {err}",
    );
}

#[test]
fn test_workflow_cache_without_workflows_fails() {
    let result = WorkerConfigBuilder::default()
        .namespace("default")
        .task_queue("test-queue")
        .versioning_strategy(default_versioning_strategy())
        .task_types(WorkerTaskTypes::ACTIVITIES)
        .max_cached_workflows(10usize)
        .build();

    assert!(
        result.is_err(),
        "Workflow cache > 0 without workflows should fail"
    );
    let err = result.err().unwrap().to_string();
    assert!(
        err.contains("max_cached_workflows"),
        "Error should mention max_cached_workflows: {err}",
    );
}

#[test]
fn test_all_combinations() {
    let combinations = [
        (WorkerTaskTypes::WORKFLOWS, "workflows only"),
        (WorkerTaskTypes::ACTIVITIES, "activities only"),
        (WorkerTaskTypes::NEXUS, "nexus only"),
        (
            WorkerTaskTypes::WORKFLOWS | WorkerTaskTypes::ACTIVITIES,
            "workflows + activities",
        ),
        (
            WorkerTaskTypes::WORKFLOWS | WorkerTaskTypes::NEXUS,
            "workflows + nexus",
        ),
        (
            WorkerTaskTypes::ACTIVITIES | WorkerTaskTypes::NEXUS,
            "activities + nexus",
        ),
        (WorkerTaskTypes::all(), "all types"),
    ];

    for (task_types, description) in combinations {
        let config = WorkerConfigBuilder::default()
            .namespace("default")
            .task_queue("test-queue")
            .versioning_strategy(default_versioning_strategy())
            .task_types(task_types)
            .build()
            .unwrap_or_else(|e| panic!("Failed to build config for {description}: {e:?}"));

        let effective = config.task_types;
        assert_eq!(
            effective, task_types,
            "Effective types should match for {description}",
        );
    }
}
