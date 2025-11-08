# Interceptors and context propagation

Interceptors are a small, typed middleware API for wrapping behaviour without forking
Loomwork: a step-attempt interceptor sees every step attempt a worker runs, and a
client interceptor sees every outbound call a `Client` or `WorkflowHandle` makes. Both
live in `loomwork.interceptors`.

Loomwork uses the same mechanism to propagate distributed traces: a run started inside
an active span, or one that already carries a `traceparent`, makes every step it
schedules a descendant of that trace. This page covers both together, since the
built-in tracing support is the clearest example of the API.

## Step interceptors

```python
import contextlib
from collections.abc import Iterator

from loomwork.interceptors import StepAttemptContext, StepInterceptor


class AuthContext:
    """Wraps every step attempt to attach a request-scoped auth token."""

    @contextlib.contextmanager
    def intercept_step(self, ctx: StepAttemptContext) -> Iterator[None]:
        token = ctx.headers.get("authorization")
        reset = current_user.set(look_up(token)) if token else None
        try:
            yield
        except Exception:
            logger.warning("step %s failed for %s", ctx.info.step_name, ctx.info.run_id)
            raise
        finally:
            if reset is not None:
                current_user.reset(reset)
```

`intercept_step` returns a context manager (write it with `@contextlib.contextmanager`,
as above): code before `yield` runs just before the step function executes, code after
`yield` runs just after it returns or raises. `StepAttemptContext` gives you:

- `info`: the same `StepInfo` the step sees through `loomwork.step_info()` — workflow
  and run id, step name, sequence number, attempt.
- `args`: the step's decoded arguments, as a tuple.
- `headers`: the context propagated from the workflow that scheduled this step (see
  [Trace propagation](#trace-propagation) below).

Several interceptors compose like `CompositeTelemetry` and are entered outermost first:
with `[a, b]`, the order is *a* enters, *b* enters, the step runs, *b* exits, *a* exits.
An interceptor may observe an exception (to log it, tag a metric, close a span with an
error status) but must not swallow it — returning a truthy value from `__exit__`
(equivalently, not re-raising after an `except`) would silently turn a failed attempt
into a successful one and corrupt retry counting and failure reporting.

Register step interceptors on the worker:

```python
worker_options = lw.WorkerOptions(step_interceptors=[AuthContext(), MetricsTagger()])
app = lw.Loomwork(..., worker_options=worker_options)
# or, as a shorthand when you have no other WorkerOptions to set:
app = lw.Loomwork(..., step_interceptors=[AuthContext()])
```

## Client interceptors

A client interceptor wraps one outbound call made through `Client` or a
`WorkflowHandle`: `start`, `signal_with_start`, `signal` or `update`. It has the same
context-manager shape:

```python
from loomwork.interceptors import ClientCallContext, ClientInterceptor


class RequestIdHeader:
    def intercept_call(self, ctx: ClientCallContext) -> contextlib.AbstractContextManager[None]:
        ctx.headers.setdefault("x-request-id", current_request_id())
        return contextlib.nullcontext()
```

`ClientCallContext.headers` is a mutable `dict[str, str]`; interceptors add to it
*before* the call is sent. What happens to it afterwards depends on `ctx.kind`:

- `"start"` and `"signal_with_start"`: if a new run is created, `headers` becomes that
  run's `WorkflowStarted.headers` — visible to workflow code, and the seed for every
  step's propagated context (see below). If the call joins an already-running
  execution instead (`signal_with_start` on a running workflow, or `start` with
  `IdConflictPolicy.USE_EXISTING`), the headers are not recorded anywhere.
- `"signal"` and `"update"`: there is no event type that carries headers into an
  already-running workflow's history (`SignalReceived`/`UpdateRequested` do not have a
  `headers` field), so they are visible only to this call's own interceptors — for
  example to open and close a client-side span around the call, as the built-in
  `TracingClientInterceptor` does.

Register client interceptors on the client or the embedded app:

```python
client = Client(engine, registry=registry, interceptors=[RequestIdHeader()])
app = lw.Loomwork(..., client_interceptors=[RequestIdHeader()])
```

## Why there is no workflow-side interceptor

Workflow code must produce the exact same commands on every replay. A general
"wrap every workflow activation" hook would be an easy way to break that invisibly: an
interceptor that reads an environment variable, checks a feature flag, or times out
differently between the original run and a later replay would pass code review and
then desynchronize a run's history the first time its decision changed. So there isn't
one. If you need context inside workflow code, read it — it is a pure function of
already-recorded state, and therefore always replay-safe:

```python
@lw.workflow
async def process_order(order_id: str) -> None:
    headers = lw.info().headers          # this run's own propagated context
    context = lw.current_trace_context()  # parsed, or None if nothing was propagated
    ...
```

## Trace propagation

`loomwork.observability.propagation` implements [W3C Trace
Context](https://www.w3.org/TR/trace-context/) (`traceparent`/`tracestate`) parsing and
formatting, plus `derived_span_id`, a pure function that turns stable inputs (a run id,
a kind, a sequence number) into a span id. Everything else described here is built on
those two primitives; core Loomwork code never imports `opentelemetry` — only
`loomwork.observability.otel` does, so tracing support costs nothing unless you use it.

### How context reaches a run

`Engine.start_workflow`, `Client.start`, `Loomwork.start_workflow` and
`signal_with_start` all accept `headers: dict[str, str] | None`. Pass a `traceparent`
directly if you already have one (for example forwarded from an inbound HTTP request),
or register the built-in `TracingClientInterceptor`, which captures the *active*
OpenTelemetry span at call time and injects it automatically:

```python
from loomwork.observability import TracingClientInterceptor

app = lw.Loomwork(..., client_interceptors=[TracingClientInterceptor()])

with tracer.start_as_current_span("checkout"):
    await app.start_workflow(process_order, order_id)  # joins the "checkout" trace
```

The headers on `WorkflowStarted` are that run's *own* propagated context
(`WorkflowInstance.trace`, parsed once at construction). Everything downstream derives
from it, deterministically:

| Path            | What it gets                                                                 |
| ---------------- | ----------------------------------------------------------------------------- |
| Each step        | a child of the run's context, span id `derived_span_id(run_id, "step", seq)`  |
| A child workflow | a child of the run's context, span id `derived_span_id(run_id, "child", seq)` |
| Continue-as-new  | the *same* context, unchanged (it is the same logical run continuing)         |
| A workflow retry | the same context as the run it retries (same mechanism as continue-as-new)    |

Deriving a span id instead of drawing a random one is what makes this replay-safe:
workflow code runs again every time a worker replays history, and a `ScheduleStep`
command it produces must match the recorded `StepScheduled` event exactly, including
on a cold worker that never saw this run before. `derived_span_id` depends only on the
run id (fixed at start) and the sequence number (assigned deterministically as the
workflow schedules things), so replay always recomputes the identical value — this is
covered by `tests/integration/test_tracing.py`, which evicts the replay cache between
two workers and checks the recorded and replayed `StepScheduled` headers are identical.

A step's own span, and any span its own code creates via an instrumented HTTP client or
database driver, are still ordinary, randomly-generated OpenTelemetry spans: only their
*parent* position is deterministic, not their own id. That parent is a legitimate,
if unusual, external context — the same mechanism OpenTelemetry uses across a real
network hop (a `NonRecordingSpan` built from a remote `SpanContext`) — so a durable
workflow spanning minutes, days or months is no different, tracing-wise, from a
synchronous call chain.

### Reading it back

- `loomwork.info().headers` and `loomwork.current_trace_context()`: the run's own
  context, from workflow code.
- `loomwork.step_info().headers` and `loomwork.current_trace_context()`: the context
  propagated to the running step attempt, from step code.
- `OpenTelemetryTelemetry`'s `step <name>` spans are automatically parented on it (see
  `docs/operations/monitoring.md`); nothing else to configure.

### Structured logging correlation

`CorrelationFilter` (`loomwork.observability`) is a `logging.Filter` that stamps
`workflow_id`, `run_id`, `step_name`, `step_attempt`, `trace_id` and `span_id` onto log
records emitted while a workflow or step is running — for ordinary
`logging.getLogger(__name__)` loggers used by your own code or a library, the same way
`loomwork.logger` already tags its own records. Attach it to a handler or a logger:

```python
handler.addFilter(CorrelationFilter())
```

See `docs/operations/monitoring.md` for the full field list and the logs it applies to
by default.
