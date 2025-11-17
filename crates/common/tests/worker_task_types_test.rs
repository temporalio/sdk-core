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
        .task_types(WorkerTaskTypes::all())
        .build()
        .expect("Failed to build default config");

    let effective = &config.task_types;
    assert!(
        effective.enable_workflows,
        "Should poll workflows by default"
    );
    assert!(
        effective.enable_activities,
        "Should poll activities by default"
    );
    assert!(effective.enable_nexus, "Should poll nexus by default");
}

#[test]
fn test_empty_task_types_fails_validation() {
    let result = WorkerConfigBuilder::default()
        .namespace("default")
        .task_queue("test-queue")
        .versioning_strategy(default_versioning_strategy())
        .task_types(WorkerTaskTypes {
            enable_workflows: false,
            enable_activities: false,
            enable_nexus: false,
        })
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
        .task_types(WorkerTaskTypes::activity_only())
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
        (WorkerTaskTypes::workflow_only(), "workflows only"),
        (WorkerTaskTypes::activity_only(), "activities only"),
        (WorkerTaskTypes::nexus_only(), "nexus only"),
        (
            WorkerTaskTypes {
                enable_workflows: true,
                enable_activities: true,
                enable_nexus: false,
            },
            "workflows + activities",
        ),
        (
            WorkerTaskTypes {
                enable_workflows: true,
                enable_activities: false,
                enable_nexus: true,
            },
            "workflows + nexus",
        ),
        (
            WorkerTaskTypes {
                enable_workflows: false,
                enable_activities: true,
                enable_nexus: true,
            },
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
