use criterion::{criterion_group, criterion_main, Criterion};
use futures::StreamExt;
use std::time::Duration;
use temporal_sdk::{WfContext, Worker, WorkflowFunction};
use temporal_sdk_core::{telemetry_init, TelemetryOptionsBuilder};
use temporal_sdk_core_protos::DEFAULT_WORKFLOW_TYPE;
use temporal_sdk_core_test_utils::{canned_histories, init_core_replay_preloaded};

pub fn criterion_benchmark(c: &mut Criterion) {
    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    telemetry_init(&TelemetryOptionsBuilder::default().build().unwrap()).unwrap();
    let _g = tokio_runtime.enter();

    let num_timers = 10;
    let t = canned_histories::long_sequential_timers(num_timers as usize);
    let hist = t.get_full_history_info().unwrap().into();

    c.bench_function("Small history replay", |b| {
        b.iter(|| {
            tokio_runtime.block_on(async {
                let func = timers_wf(num_timers);
                let (worker, _) = init_core_replay_preloaded("replay_bench", &hist);
                let mut worker = Worker::new_from_core(worker, "replay_bench".to_string());
                worker.register_wf(DEFAULT_WORKFLOW_TYPE, func);
                worker.run().await.unwrap();
            })
        })
    });

    let num_tasks = 50;
    let t = canned_histories::lots_of_big_signals(num_tasks);
    let hist = t.get_full_history_info().unwrap().into();

    c.bench_function("Large payloads history replay", |b| {
        b.iter(|| {
            tokio_runtime.block_on(async {
                let func = big_signals_wf(num_tasks);
                let (worker, _) = init_core_replay_preloaded("large_hist_bench", &hist);
                let mut worker = Worker::new_from_core(worker, "large_hist_bench".to_string());
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
