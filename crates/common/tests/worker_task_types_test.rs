use std::collections::HashSet;
use temporalio_common::worker::{
    WorkerConfigBuilder, WorkerTaskType, WorkerTaskTypes, WorkerVersioningStrategy,
    worker_task_types,
};

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

    let effective = &config.task_types;
    assert!(
        effective.contains(&WorkerTaskType::Workflows),
        "Should poll workflows by default"
    );
    assert!(
        effective.contains(&WorkerTaskType::Activities),
        "Should poll activities by default"
    );
    assert!(
        effective.contains(&WorkerTaskType::Nexus),
        "Should poll nexus by default"
    );
}

#[test]
fn test_workflow_only_worker() {
    let config = WorkerConfigBuilder::default()
        .namespace("default")
        .task_queue("test-queue")
        .versioning_strategy(default_versioning_strategy())
        .task_types(HashSet::from([WorkerTaskType::Workflows]))
        .max_cached_workflows(0usize)
        .build()
        .expect("Failed to build workflow-only config");

    let effective = &config.task_types;
    assert!(
        effective.contains(&WorkerTaskType::Workflows),
        "Should poll workflows"
    );
    assert!(
        !effective.contains(&WorkerTaskType::Activities),
        "Should NOT poll activities"
    );
    assert!(
        !effective.contains(&WorkerTaskType::Nexus),
        "Should NOT poll nexus"
    );
}

#[test]
fn test_activity_and_nexus_worker() {
    let types: WorkerTaskTypes = [WorkerTaskType::Activities, WorkerTaskType::Nexus]
        .into_iter()
        .collect();
    let config = WorkerConfigBuilder::default()
        .namespace("default")
        .task_queue("test-queue")
        .versioning_strategy(default_versioning_strategy())
        .task_types(types)
        .max_cached_workflows(0usize)
        .build()
        .expect("Failed to build activity+nexus config");

    let effective = &config.task_types;
    assert!(
        !effective.contains(&WorkerTaskType::Workflows),
        "Should NOT poll workflows"
    );
    assert!(
        effective.contains(&WorkerTaskType::Activities),
        "Should poll activities"
    );
    assert!(
        effective.contains(&WorkerTaskType::Nexus),
        "Should poll nexus"
    );
}

#[test]
fn test_empty_task_types_fails_validation() {
    let result = WorkerConfigBuilder::default()
        .namespace("default")
        .task_queue("test-queue")
        .versioning_strategy(default_versioning_strategy())
        .task_types(WorkerTaskTypes::new())
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
        .task_types(worker_task_types::activities())
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
        (worker_task_types::workflows(), "workflows only"),
        (worker_task_types::activities(), "activities only"),
        (worker_task_types::nexus(), "nexus only"),
        (
            [WorkerTaskType::Workflows, WorkerTaskType::Activities]
                .into_iter()
                .collect(),
            "workflows + activities",
        ),
        (
            [WorkerTaskType::Workflows, WorkerTaskType::Nexus]
                .into_iter()
                .collect(),
            "workflows + nexus",
        ),
        (
            [WorkerTaskType::Activities, WorkerTaskType::Nexus]
                .into_iter()
                .collect(),
            "activities + nexus",
        ),
        (worker_task_types::all(), "all types"),
    ];

    for (task_types, description) in combinations {
        let config = WorkerConfigBuilder::default()
            .namespace("default")
            .task_queue("test-queue")
            .versioning_strategy(default_versioning_strategy())
            .task_types(task_types.clone())
            .build()
            .unwrap_or_else(|e| panic!("Failed to build config for {description}: {e:?}"));

        let effective = config.task_types;
        assert_eq!(
            effective, task_types,
            "Effective types should match for {description}",
        );
    }
}
