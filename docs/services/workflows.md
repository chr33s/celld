# Workflows

A Workflow is a durable function that runs as a named sequence of steps,
sleeps, and events. celld runs each instance as a cell. Read the
[Cloudflare Workflows documentation](https://developers.cloudflare.com/workflows/build/workers-api/)
for the API.

## Example

The [Workflow example](../../examples/workflow) fetches a document and stores
the result of a durable step.

<!-- celld-example: workflow -->

## API

- A class extends `WorkflowEntrypoint` and implements `run(event, step)`.
  `event.payload` holds the parameters, and the return value becomes the
  `output` field of `status()`.
- `step.do(name, callback)` runs a durable step, and it retries the callback on
  failure. A final `{ rollback, rollbackConfig }` argument registers a
  compensating handler for the step.
- `step.sleep(name, duration)` and `step.sleepUntil(name, timestamp)` stop the
  instance until a deadline.
- `step.waitForEvent(name, options)` stops the instance until a matching event.
- `env.MY_WORKFLOW.create()`, `createBatch()`, and `get(id)` return an
  instance. `deleteBatch()` deletes several instances.
- An instance has `status()`, `sendEvent()`, `pause()`, `resume()`,
  `restart()`, `terminate()`, and `delete()`. `terminate({ rollback: true })`
  runs the registered rollback handlers before the instance ends. celld adds
  `retryRollback()` (see [Rollback](#rollback)).

## Replay

Each time an instance makes progress, celld calls `run()` again from the first
line. A finished step returns its stored result and does not run its callback
again. All code outside a step callback therefore runs again on every replay.
Put each subrequest, each side effect, and each value that must stay stable
inside a `step.do()` callback, as the
[rules of Workflows](https://developers.cloudflare.com/workflows/build/rules-of-workflows/)
require. A step whose result did not commit before a node failure runs again,
so a step callback must tolerate a second attempt.

`run()` can await work that is not a step, but a replay cannot resume that
await. celld fails the instance when such work keeps `run()` pending for 60
seconds while no step runs or waits.

## Sleeps, events, and retries

`step.waitForEvent()` times out after 24 hours by default. An event that
arrives before the instance reaches its wait step is buffered. A sleep, a wait,
and a pending retry each store their deadline, so a crash or a slow replay
cannot move the deadline. `status()` reports `waiting` for all three.

A waiting instance holds no isolate. Its cell hibernates when the next deadline
is further away than the near-alarm residency window. That window is one hour,
and `CELLD_ALARM_RESIDENT_MS` changes it.

`step.do()` uses the Cloudflare retry defaults when the call supplies no
`retries` object: 5 retries, a delay of 10 seconds, exponential backoff, and a
timeout of 10 minutes for one attempt.

## Rollback

When `run()` fails with an error that it does not catch, celld runs the
rollback handler of each step that started with one, latest-started step
first. A handler receives `{ ctx, error, output }`: the forward step's
context, the error that failed the instance, and the step's stored result.
`output` is `undefined` for a step that failed or had not finished, because
its effect may still have happened. A failure that `run()` catches, a
successful instance, a plain `terminate()`, and `delete()` run no handler.

`rollbackConfig` takes the `retries` and `timeout` options of a step, and the
step defaults apply to a handler without them. Each handler attempt, its
backoff deadline, and its result are stored in the instance, so a crash
resumes the rollback where it stopped. A handler can therefore run more than
once, and a timed-out attempt can still be running when the next one starts.
Make each handler idempotent, for example with a provider idempotency key.

celld rebuilds the handlers by calling `run()` again without running any step
callback. A step that needs a rollback but whose handler the deployed code no
longer registers, or whose `rollbackConfig` changed, fails the rollback rather
than running a mismatched handler.

`status()` reports `running` while a rollback runs, then the status the
instance would have had, with `rollback: { outcome, error }`. A handler that
exhausts its retries stops the rollback: the earlier steps keep their
effects, and `outcome` is `"failed"`. `terminate({ rollback: true })` resolves
after the rollback settles, also when it fails, or after 10 seconds while it
still runs; check `status()` for the outcome. It works on a paused instance
without resuming it. While a rollback runs, `sendEvent()`, `pause()`,
`restart()`, and `terminate()` are rejected, and `delete()` stops it. A step
callback that terminates its own instance with `rollback: true` waits for
its step timeout, because the rollback starts only after every running step
callback settles.

`retryRollback()` resumes a rollback whose outcome is `"failed"`, under the
deployed code and `rollbackConfig`. Completed handlers do not run again, and
the step that failed gets a new retry budget. It resolves like
`terminate({ rollback: true })`.

celld sets no limit on the number of concurrent instances. Fleet memory and the
resident-cell cap bound it.

## Differences from Cloudflare

- celld retains a successful or failed instance for 30 days by default. Each
  duration in the `retention` option can be at most 30 days.
- `locationHint` accepts the Cloudflare values, but fleet ownership selects the
  cell location.
- Non-step work cannot remain pending for more than 60 seconds.
- A step result, an event payload, and the workflow parameters each have a
  1 MiB limit.
- A sensitive step result and a `ReadableStream` step result are unavailable.
- `retryRollback()` is a celld extension; Cloudflare has no way to resume a
  failed rollback. `restart()` is rejected while a rollback runs.
- Rollback orders steps by their first attempt's start. Cloudflare orders a
  retried step by its latest attempt, so the two can differ when a retried
  step started after a sibling.
- Rollback lifecycle events go to the node log as `event="workflow_rollback"`
  records, not to Workflows analytics. Each names its `kind` (`started`,
  `step_started`, `attempt_started`, `attempt_completed`, `attempt_errored`,
  `step_completed`, `step_errored`, `completed`, `errored`, or `retried`),
  the instance, the step, the attempt, and the error name. A record never
  holds a step output or an error message. A crash can repeat a record.
- `restart({ from })` runs again any earlier step whose rollback completed,
  instead of replaying its stored result.
- A `workflows` entry cannot carry `schedules`, `limits`, or a `script_name`
  that names another script.
- The Workflows REST API and the `wrangler workflows` commands are
  unavailable. Drive an instance through the binding.

The [Cloudflare compatibility](../cloudflare-compat.md#services) page lists
the runtime APIs and the unsupported services.
