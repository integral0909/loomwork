# Task queues

Routing GPU-heavy work to a dedicated worker, with a per-queue concurrency limit, and
two `Worker`s in one process sharing a SQLite store.

## Scenario

`render_video` (in `workflows.py`) renders a video: extract metadata, render every
frame, then mux the result. `render_frame` (in `steps.py`) is declared with
`queue="gpu"`:

```python
@lw.step(name="task_queues.render_frame", queue="gpu")
async def render_frame(video_id: str, frame: int) -> str: ...
```

`main.py` starts two `Loomwork` instances against the *same* `SQLiteStore` file --
one process, two workers, sharing durable state exactly as two machines in a real
fleet would share a database:

- the **general** worker subscribes only to `"default"` (via
  `WorkerOptions(task_queues=("default",))`) and processes the workflow's own ticks
  plus its two cheap steps;
- the **gpu** worker subscribes only to `"gpu"`, sets `workflow_concurrency=0` (it
  never processes ticks, only steps), and caps the queue at `QueueLimit(concurrency=2)`
  -- modeling a host with two GPU slots.

Because only the gpu worker polls the `"gpu"` queue, `render_frame` attempts sit in the
store until that worker claims them, however many other workers (or other queues) are
busy; because of the concurrency limit, at most two attempts run on it at once no
matter how many frames a video has.

## Loomwork features used

- `@loomwork.step(queue=...)` to route a step to a specific task queue.
- `WorkerOptions(task_queues=...)` to make a worker subscribe to only some queues.
- `WorkerOptions(queue_limits={...})` with `QueueLimit(concurrency=...)` for a
  worker-local cap on one queue (a fleet-wide limit is this times the number of
  workers that poll the queue).
- Two `Loomwork` instances sharing one `SQLiteStore`, each owning a different `Worker`.

## Running it

```console
uv run python -m examples.task_queues.main
```

This uses a real `SQLiteStore` in a temporary directory (task queues and multi-worker
coordination are exactly the kind of thing worth seeing against real persistence
rather than the in-memory store), but nothing in the scenario waits on hours or days,
so it runs against the real clock and finishes in well under a second.

## Tests

`tests/examples/test_task_queues.py` checks:

- `render_frame` attempts are only ever claimed by the worker subscribed to `"gpu"`,
  never by the general-queue worker;
- no more than two renders run at once, matching the configured `QueueLimit`.
