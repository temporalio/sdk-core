use criterion::{criterion_group, criterion_main, Criterion};
use futures::StreamExt;
use std::time::Duration;
use temporal_sdk::{WfContext, WorkflowFunction};
use temporal_sdk_core::replay::HistoryForReplay;
use temporal_sdk_core_protos::DEFAULT_WORKFLOW_TYPE;
use temporal_sdk_core_test_utils::{canned_histories, replay_sdk_worker};

pub fn criterion_benchmark(c: &mut Criterion) {
    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let _g = tokio_runtime.enter();

    let num_timers = 10;
    let t = canned_histories::long_sequential_timers(num_timers as usize);
    let hist = HistoryForReplay::new(
        t.get_full_history_info().unwrap().into(),
        "whatever".to_string(),
    );

    c.bench_function("Small history replay", |b| {
        b.iter(|| {
            tokio_runtime.block_on(async {
                let func = timers_wf(num_timers);
                let mut worker = replay_sdk_worker([hist.clone()]);
                worker.register_wf(DEFAULT_WORKFLOW_TYPE, func);
                worker.run().await.unwrap();
            })
        })
    });

    let num_tasks = 50;
    let t = canned_histories::lots_of_big_signals(num_tasks);
    let hist = HistoryForReplay::new(
        t.get_full_history_info().unwrap().into(),
        "whatever".to_string(),
    );

    c.bench_function("Large payloads history replay", |b| {
        b.iter(|| {
            tokio_runtime.block_on(async {
                let func = big_signals_wf(num_tasks);
                let mut worker = replay_sdk_worker([hist.clone()]);
                worker.register_wf(DEFAULT_WORKFLOW_TYPE, func);
                worker.run().await.unwrap();
            })
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

fn timers_wf(num_timers: u32) -> WorkflowFunction {
    WorkflowFunction::new(move |ctx: WfContext| async move {
        for _ in 1..=num_timers {
            ctx.timer(Duration::from_secs(1)).await;
        }
        Ok(().into())
    })
}

fn big_signals_wf(num_tasks: usize) -> WorkflowFunction {
    WorkflowFunction::new(move |ctx: WfContext| async move {
        let mut sigs = ctx.make_signal_channel("bigsig");
        for _ in 1..=num_tasks {
            for _ in 1..=5 {
                let _ = sigs.next().await.unwrap();
            }
        }

        Ok(().into())
    })
}
