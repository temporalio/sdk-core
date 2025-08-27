# Intro to Temporal SDKs

This document is primarily meant as an onboarding guide for new Temporal SDK developers, to be
accompanied by a live presentation. Hence, content does not have maximal context. If reading this
independently, you will also want to refer to our documentation [site](https://docs.temporal.io).

## Primary SDK Concepts

See also
our [official docs on basic concepts](https://docs.temporal.io/evaluate/understanding-temporal).

### Workflows

Workflows are durable programs. Their state is materialized from event history and they can be
replayed zero or more times. Workflow code must be deterministic and must use SDK workflow-specific
APIs. Side effects and non-deterministic APIs should be placed in Activities.

**Example Workflow Code (Python):**

```python
from datetime import timedelta
from temporalio import workflow

@workflow.defn
class SayHello:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            say_hello, name, schedule_to_close_timeout=timedelta(seconds=5)
        )
```

### Activities

Activities are invoked from a Workflow and can run arbitrary code. They can be retried, are subject
to timeouts, and should be idempotent/reentrant. Activities can be short or long-running and can
heartbeat.

**Example Activity Code (Python):**

```python
from temporalio import activity

@activity.defn
async def say_hello(name: str) -> str:
    return f"Hello, {name}!"
```

### Clients

Clients serve as a driver for interaction with the Temporal server. They provide a high-level API
for a subset of gRPC calls, including Workflow "CRUD" operations, Activity completion, and
Schedules.

**Example Client Code (Python):**

``` python
from temporalio.client import Client

# Import the workflow from the previous code
from .workflows import SayHello

async def main():
    client = await Client.connect("localhost:7233")

    result = await client.execute_workflow(
        SayHello.run, "my name", id="my-workflow-id", task_queue="my-task-queue")

    print(f"Result: {result}")
```

### Workers

Workers are stateless entities that poll the server for Workflow, Activity, and Nexus tasks. They
run user-defined workflows and activities, manage concurrency and rate limits, and provide the
language runtime for workflows.

**Example Worker Code (Python):**

``` python
from temporalio.client import Client
from temporalio.worker import Worker

from .activities import say_hello
from .workflows import SayHello

async def main():
    client = await Client.connect("localhost:7233")

    worker = Worker(
        client,
        task_queue="my-task-queue",
        workflows=[SayHello],
        activities=[say_hello])
    await worker.run()
```

## SDK \<-\> Server Interaction

### Event History & Replay

Event history is a record of all events that have occurred in a workflow. Workflow tasks are the
fundamental unit of progress for Workflows. Any time your workflow code makes progress, it does so
because your worker processed one of these workflow tasks. Workflow tasks contain some or all of the
workflow’s history, along with possibly queries and other bits of information the worker might care
about.

The first few events in a workflow's history always look like this:

![initial_event_history.png](diagrams/initial_event_history.png)

The first workflow task for a workflow always includes the initial events. When the SDK receives a
task, it processes the history, runs your workflow code, and eventually responds with commands to
the server. Commands are actions like "start a timer" or "run an activity."

A sequence diagram of this process is shown below. Not everything may make sense yet, but we can
refer back to this diagram during later explanations.

```mermaid
---
config:
  theme: 'base'
  themeVariables:
    background: '#ffffff'
---
sequenceDiagram
    participant Client
    participant Server
    participant SDK
    participant UserCode as "User Code"
    Client ->> Server: StartWorkflowExecution RPC
    Server ->> Server: Generate Workflow Task & persist
    Server -->> Client: runID

    loop Workflow Task Polling
        rect rgb(200, 230, 200)
            SDK ->> Server: PollWorkflowTaskQueueRequest
            opt Something triggers new Workflow Task
                Server ->> Server: Generate Workflow Task & persist
            end
            Server -->> SDK: PollWorkflowTaskQueueResponse
        end

        rect rgb(220, 200, 220)
            loop Processing one Workflow Task
                SDK ->> SDK: Apply workflow history to state
                SDK ->> UserCode: Activate & Unblock Awaitables
                UserCode -->> SDK: Blocked on some awaitable or complete
            end

            SDK ->> Server: RespondWorkflowTaskCompletedRequest
            Server ->> Server: Persist & possibly reply with new Workflow Task
            Server -->> SDK: RespondWorkflowTaskCompletedResponse
        end
    end
```

**How History is Constructed:**

Workflow tasks contain history and are responded to with commands. Every command is turned into a
corresponding event (referred to as "dual" or "command-events"). The Temporal server takes action on
received commands, and the consequences of those actions also become events (e.g., `TimerFired`,
`ActivityTaskCompleted`). When events that trigger a new workflow task occur, the Temporal server
generates a new workflow task because a worker needs to run the workflow code to process this new
information.

This cycle continues until the workflow is complete: new Workflow Tasks are created, processed by
the SDK, responded to with commands, until a new workflow task begins, and so on.

We will use the following example workflow to illustrate this process:

```python
@activity.defn
async def say_hello(name: str) -> str:
    return f"Hello, {name}!"

@workflow.defn
class ReplayExampleWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        my_hello = workflow.execute_activity(
            say_hello, name, schedule_to_close_timeout=timedelta(seconds=5)
        )
        timer = asyncio.sleep(1)
        (activity_result, _) = await asyncio.gather(my_hello, timer)
        return activity_result
```

**Example Workflow History Analysis:**

1. The initial workflow task is processed. The `execute_activity` call creates a command for
   scheduling an activity, and `asyncio.sleep` (monkey-patched by Temporal) creates a command for a
   timer.
2. The `asyncio.gather` line indicates a wait for both the activity and timer to resolve.
3. The SDK knows when to respond to the task because the workflow cannot make any more progress. All
   buffered commands are sent to the server (activity and timer commands, in that order). This
   completes the first workflow task.
4. The server receives these commands, which are converted into `ActivityTaskScheduled` and
   `TimerStarted` events (duals of the commands).
5. When the activity completes, an `ActivityTaskCompleted` event is added to the history. This
   triggers a new workflow task.
6. The worker processes this new task. The workflow code, however, is still blocked on `gather`
   because the timer hasn't fired. The SDK replies with an empty list of commands, indicating no new
   actions.
7. The timer fires, resulting in a `TimerFired` event. This generates another workflow task.
8. When this task is processed, the `gather` call resolves. The workflow function returns, producing
   a `CompleteWorkflowExecution` command.
9. This command is converted into a `WorkflowExecutionCompleted` event, and the workflow finishes.

The complete history at the end of all this is the following:

1. Workflow Execution Started
2. Workflow Task Scheduled
3. Workflow Task Started
4. Workflow Task Completed
5. Activity Task Scheduled `say_hello`
6. Timer Started `1 (1s)`
7. Activity Task Started `say_hello`
8. Activity Task Completed `say_hello`
9. Workflow Task Scheduled
10. Workflow Task Started
11. Workflow Task Completed `Notice: No commands sent`
12. Timer Fired `1 (1s)`
13. Workflow Task Scheduled
14. Workflow Task Started
15. Workflow Task Completed
16. Workflow Execution Completed

This repetitive nature of processing workflow tasks, where the SDK polls for tasks, processes
history, unblocks user code, and sends new commands, continues until workflow completion.

### How Replay Works

Replay is fundamental to Temporal's durability and determinism.

**History Processing:**

SDKs process events from history serially, one at a time. Each event is fed to a specific instance
of a state machine. There is a type of state machine for every command, as well as some non-command
events/concepts. The state of a particular workflow run consists is the combination of the states of
these state machines, and importantly, the order in which they are created.

**Timer State Machine in Core (Example):**

<img src="https://uml.planttext.com/plantuml/svg/fL912iCW4Bpx2g6tWX_eeGI4Fg3s5d8mkj206h3w_oOR8q9jfFJaBhCpiuvsho1zYDQG_ZnGngwKUv01D4adPVrfD1661HBhB9-jbKud-4A5UeAE1aW5RP9JZzXZik1_KRc3chrUPP2AqB9uGu5Bfy2WELQa9baIRfFF7bWt6Pim4Zukl7b-dTYUu1_-qHe2NCYX5t15Rnqjbia9x2tPYyDiCQzAc0dkmEdbxxq1" width="500" alt="Timer FSM Diagram"/>

We can use the Timer state machine to illustrate how determinism is enforced:

When we first create the timer, we immediately schedule it, which produces the `StartTimer` command,
which becomes the `TimerStarted` event. The `Created` state isn’t really necessary here, but the
schedule transition serves as a convenient way to produce the start timer command.

When we see a `TimerStarted` event on history, we know that the command we issued exists in workflow
history, and we move to the StartCommandRecorded state and wait to see what happens - it can either
fire or be canceled.

In our example, there’s a `TimerFired` event, so when the SDK processes that event, we’d take that
transition, and the machine ends up in the final fired state

At this point, you might be wondering - why does that `StartCommandRecorded` state matter? Why have
a transition for `TimerStarted` at all? We know we sent the command to the server, so why not just
wait to see if the timer fires or is canceled from the `StartCommandCreated` state? The answer is
that's a big part of the secret sauce for how we enforce determinism.

**Enforcing Determinism:**

A workflow must always emit the same commands in the same sequence. Temporal can detect violations
of this rule because of the "dual" events that each command produces. The `TimerStarted` transition
ensures that when replaying a workflow, the current code attempting to start a timer does so at the
same point and in the same way as what exists in history. The order in which state machines are
created by workflow code also matters to ensure the same sequence of commands is produced. This
prevents scenarios where changed code might run a command that should have been mutually exclusive
with a command already in history.

**Example of Nondeterminism:**

If the order of arguments to `asyncio.gather` is swapped in the previous example workflow, replaying
the workflow will result in a nondeterminism error (often referred to as an "NDE"):

``` 
temporalio.workflow.NondeterminismError: Workflow activation completion failed: Failure { failure:
Some(Failure { message: "[TMPRL1100] Nondeterminism error: Timer machine does not handle this event:
HistoryEvent(id: 5, ActivityTaskScheduled)", source: "", stack_trace: "", encoded_attributes: None,
cause: None, failure_info: Some(ApplicationFailureInfo(ApplicationFailureInfo { r#type: "",
non_retryable: false, details: None, next_retry_delay: None })) }), force_cause:
NonDeterministicError }
```

This error occurs because the "dual" events in the history are not in the same order as the commands
produced by the modified workflow code. The timer machine, expecting a `TimerStarted` event,
encounters an `ActivityTaskScheduled` event instead, leading to a nondeterminism error. In other
words, we did not produce the same commands in the same order. This mechanism ensures workflow state
consistency.