# `loomwork.lint` rule reference

`loomwork.lint` is a static analyzer that finds non-deterministic code in `@workflow`
functions before it is deployed. Workflow code is re-executed from the start on every
replay (see [ADR-0001](adr/0001-architecture.md)), so anything that can return a
different value or perform real I/O the second time around will eventually produce a
`NondeterminismError` at runtime, or worse, silently diverge. This document lists every
built-in rule; run `python -m loomwork.lint --help` for the command line interface.

```
python -m loomwork.lint src/                 # human-readable text report
python -m loomwork.lint src/ --format json    # machine-readable
python -m loomwork.lint src/ --format sarif   # for GitHub code scanning
```

Exit codes: `0` clean, `1` findings were reported, `2` the invocation itself was wrong
(bad arguments, a path that does not exist, invalid configuration).

## How it works, briefly

The linter parses each file's AST, finds every function decorated `@workflow` (however
the decorator was imported - `@lw.workflow`, `@loomwork.sdk.workflow`,
`from loomwork import workflow as wf; @wf`, `@lw.workflow(name=...)`, ...), and checks its
body against the rules below. If a workflow calls a plain helper function defined in the
same module (one with no `@step`/`@workflow` decorator of its own), the linter follows
that call too, depth-limited, and reports the violation with a call path showing how the
workflow reaches it. `@step` bodies are never checked: steps exist precisely so that
workflow code has somewhere to put real I/O.

## Suppressing a finding

```python
time.time()  # loomwork: ignore[LW101]
time.time()  # loomwork: ignore            (silences every rule on this line)
```

```python
# loomwork: ignore-file
```
anywhere in a file silences the whole file.

## Configuration

Add a `[tool.loomwork.lint]` table to `pyproject.toml`:

```toml
[tool.loomwork.lint]
select = ["LW101", "LW102"]                       # only check these codes
ignore = ["LW104"]                                # never report these codes
extend-forbidden-calls = ["myapp.legacy.fetch"]   # extra forbidden dotted calls (LW199)
allowed-calls = ["requests.get"]                  # exempt a normally-forbidden call
exclude = ["**/generated/**"]                     # glob patterns of files to skip
```

All keys are optional. `select` and `ignore` accept any code from the table below, plus
`LW199` (see [extend-forbidden-calls](#lw199-custom-forbidden-call)) and `LW000` (a file
the linter could not parse).

## Rule table

| Code | Name | Severity | Concern |
| --- | --- | --- | --- |
| [LW000](#lw000-parse-error) | parse-error | error | file could not be parsed |
| [LW101](#lw101-wall-clock-time) | wall-clock-time | error | reading the wall clock |
| [LW102](#lw102-nondeterministic-randomness) | nondeterministic-randomness | error | unseeded randomness |
| [LW103](#lw103-filesystem-io) | filesystem-io | error | touching the filesystem |
| [LW104](#lw104-print-call) | print-call | warning | `print()` instead of `loomwork.logger` |
| [LW105](#lw105-blocking-stdin) | blocking-stdin | error | reading interactive input |
| [LW106](#lw106-network-io) | network-io | error | network I/O |
| [LW107](#lw107-subprocess-io) | subprocess-io | error | running an external process |
| [LW108](#lw108-database-io) | database-io | error | talking to a database directly |
| [LW109](#lw109-blocking-concurrency) | blocking-concurrency | error | threads, pools, executors |
| [LW110](#lw110-environment-access) | environment-access | warning | reading an environment variable |
| [LW111](#lw111-hash-order-iteration) | hash-order-iteration | warning | iterating a `set`/`frozenset` |
| [LW112](#lw112-hash-in-control-flow) | hash-in-control-flow | warning | `id()`/`hash()` driving a branch |
| [LW113](#lw113-global-mutation) | global-mutation | error | assigning to a module global |
| [LW199](#lw199-custom-forbidden-call) | custom-forbidden-call | error | project-specific forbidden call |

---

### LW000: parse-error

**What:** the file could not be parsed as Python at all (or could not be read, for
example due to an encoding problem).

**Why:** every other rule needs a valid AST; if parsing fails silently, a file with a
syntax error would look "clean" simply because nothing in it was ever checked.

**Bad:**
```python
def process(:  # syntax error
    ...
```

**Good:** fix the syntax error. `LW000` is not about workflow code; it fires the same
way for any unparsable file.

---

### LW101: wall-clock-time

**What:** flags `time.time()`, `time.monotonic()`, `time.monotonic_ns()`,
`time.perf_counter()`, `time.perf_counter_ns()`, `time.time_ns()`, `time.sleep()`,
`datetime.datetime.now()`, `datetime.datetime.utcnow()`, `datetime.datetime.today()` and
`datetime.date.today()`.

**Why:** these read the operating system's wall clock, which returns a different value
every time it is called - including on replay, when the workflow function runs again
from the top.

**Bad:**
```python
@workflow
async def process(order: Order) -> None:
    started_at = datetime.now()
    ...
```

**Good:**
```python
@workflow
async def process(order: Order) -> None:
    started_at = loomwork.now()      # the current tick's time, stable across replay
    await loomwork.sleep(60)         # durable delay instead of time.sleep()
```

`asyncio.get_event_loop().time()` is deliberately **not** flagged: inside a workflow it
returns `DeterministicEventLoop`'s own clock (see
[ADR-0001](adr/0001-architecture.md)), which is already deterministic.

---

### LW102: nondeterministic-randomness

**What:** flags any use of the `random` or `secrets` modules, plus `uuid.uuid1()`,
`uuid.uuid4()` and `os.urandom()`.

**Why:** these are unseeded (or time/host seeded) and produce a different result on
replay than they did during the original run.

**Bad:**
```python
@workflow
async def process() -> None:
    if random.random() < 0.1:
        ...
    ticket_id = uuid.uuid4()
```

**Good:**
```python
@workflow
async def process() -> None:
    if loomwork.random().random() < 0.1:   # seeded per run; same sequence on replay
        ...
    ticket_id = loomwork.uuid4()           # replay-stable
```

`uuid.uuid3()` and `uuid.uuid5()` are **not** flagged: they are pure functions of a
namespace and a name, so they already produce the same value every time.

---

### LW103: filesystem-io

**What:** flags `open()`, the filesystem-touching methods of `pathlib.Path` (`read_text`,
`write_text`, `read_bytes`, `write_bytes`, `open`, `exists`, `is_file`, `is_dir`,
`is_symlink`, `mkdir`, `rmdir`, `unlink`, `iterdir`, `glob`, `rglob`, `stat`, `lstat`,
`chmod`, `touch`, `rename`, `replace`, `symlink_to`, `hardlink_to`, `samefile`, `walk`),
the equivalent functions in `os` and `os.path`, and anything from `shutil`.

**Why:** the filesystem is not part of the recorded history; a file that existed during
the original run may not exist (or may have different contents) during a replay on a
different worker.

**Bad:**
```python
@workflow
async def process(path: str) -> str:
    return Path(path).read_text()
```

**Good:** move the read into a step, so the *result* is journaled instead of the read
being repeated:
```python
@step
async def read_file(path: str) -> str:
    return Path(path).read_text()

@workflow
async def process(path: str) -> str:
    return await read_file(path)
```

Pure path manipulation (joining, splitting, `PurePath` operations) never touches disk and
is not flagged.

---

### LW104: print-call

**What:** flags `print()`.

**Why:** `print()` output is not part of the workflow's history and, unlike
`loomwork.logger`, is not automatically suppressed during replay - so every replay
reprints it.

**Bad:**
```python
@workflow
async def process(order: Order) -> None:
    print(f"processing {order.id}")
```

**Good:**
```python
@workflow
async def process(order: Order) -> None:
    loomwork.logger.info("processing %s", order.id)
```

This is a warning, not an error: it is easy to fix and never actually breaks replay
(unlike the other I/O rules), it just produces noisy or duplicated logs.

---

### LW105: blocking-stdin

**What:** flags `input()`, `sys.stdin.read()` and `sys.stdin.readline()`.

**Why:** interactive input has no meaning for a durable, possibly-distributed workflow,
and no replay can supply it a second time.

**Bad:**
```python
@workflow
async def process() -> None:
    answer = input("continue? ")
```

**Good:** model the interaction as a signal or an external event the workflow awaits,
handled by application code outside the workflow - not as a blocking read.

---

### LW106: network-io

**What:** flags any call into `socket`, `requests`, `httpx`, `aiohttp`,
`urllib.request`, `http.client`, `ftplib`, `smtplib`, `websocket` or `websockets`.

**Why:** the response depends on a remote system's state at a specific instant; asking
again on replay can return something different, or fail entirely.

**Bad:**
```python
@workflow
async def process(order_id: str) -> dict:
    return requests.get(f"https://api.example.com/orders/{order_id}").json()
```

**Good:**
```python
@step
async def fetch_order(order_id: str) -> dict:
    return requests.get(f"https://api.example.com/orders/{order_id}").json()

@workflow
async def process(order_id: str) -> dict:
    return await fetch_order(order_id)
```

---

### LW107: subprocess-io

**What:** flags anything from `subprocess`, plus `os.system()` and `os.popen()`.

**Why:** running an external process is I/O with arbitrary, unjournaled side effects and
an outcome that cannot be replayed.

**Bad:**
```python
@workflow
async def process() -> str:
    return subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True).stdout
```

**Good:** move the subprocess call into a step, exactly as with network or filesystem
I/O.

---

### LW108: database-io

**What:** flags calls into `sqlite3`, `psycopg`, `psycopg2`, `asyncpg`, `pymongo`,
`motor`, `redis`, `pymysql`, `mysql.connector`, `pyodbc`, or `sqlalchemy.create_engine`.

**Why:** a database query's result is external state, not something recorded in the
workflow's own history; querying again on replay is not guaranteed to see the same rows.

**Bad:**
```python
@workflow
async def process(user_id: str) -> str:
    conn = sqlite3.connect("app.db")
    return conn.execute("SELECT email FROM users WHERE id = ?", (user_id,)).fetchone()[0]
```

**Good:** put the query in a step and pass its (journaled) result back to the workflow.

---

### LW109: blocking-concurrency

**What:** flags `threading.*`, `concurrent.futures.*`, `multiprocessing.*`,
`asyncio.to_thread()`, and any call to a method named `run_in_executor` (regardless of
what it is called on, since it is almost always an opaque `asyncio` event loop object).

**Why:** `DeterministicEventLoop` (see
[ADR-0001](adr/0001-architecture.md)) already refuses these at runtime with a
`DeterminismViolation`, because they hand control to a real OS thread or process outside
the deterministic tick loop; this rule catches the same mistake before deploy instead of
when a workflow crashes mid-tick.

**Bad:**
```python
@workflow
async def process() -> str:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, blocking_call)
```

**Good:** make `blocking_call` a step; steps already run off the workflow loop, with no
special API needed:
```python
@step
def blocking_call() -> str: ...

@workflow
async def process() -> str:
    return await blocking_call()
```

---

### LW110: environment-access

**What:** flags `os.environ` (however it is used: subscript, `.get()`, iteration, or
passed around) and `os.getenv()`.

**Why:** an environment variable is ambient, unjournaled input to the process. It is
often stable for the process's whole lifetime, which is why this is a warning rather than
an error, but it *can* still differ between the worker that ran the workflow originally
and the one that replays it.

**Bad:**
```python
@workflow
async def process() -> None:
    if os.environ["FEATURE_X_ENABLED"] == "true":
        ...
```

**Good:** read configuration once at process startup, outside any workflow, and pass the
value in as a workflow argument or through a step; or read it inside a step if it
genuinely needs to be looked up per-run.

---

### LW111: hash-order-iteration

**What:** flags `for`/`async for` directly over a `set` literal, a `set(...)` or
`frozenset(...)` call, or a set comprehension.

**Why:** Python's iteration order for sets depends on the hashes of their elements which,
for `str`, `bytes` and a few other types, are randomized per process via
`PYTHONHASHSEED`. Two runs of the *same* code can iterate the same set in a different
order.

**Bad:**
```python
@workflow
async def process(tags: set[str]) -> list[str]:
    return [tag for tag in tags]   # order is not reproducible
```

**Good:**
```python
@workflow
async def process(tags: set[str]) -> list[str]:
    return [tag for tag in sorted(tags)]
```

Iterating a `list`, `tuple` or `dict` (whose insertion order is preserved and is not
hash-seed dependent) is not flagged, and neither is `sorted(some_set)`.

---

### LW112: hash-in-control-flow

**What:** flags `id()` or `hash()` calls that appear directly in the condition of an
`if`, `while`, or a conditional expression (`x if cond else y`).

**Why:** both are randomized per process (`hash()` via `PYTHONHASHSEED`, `id()` via
memory layout) and must never decide which branch a workflow takes.

**Bad:**
```python
@workflow
async def process(a: Task, b: Task) -> None:
    if hash(a) < hash(b):
        ...
```

**Good:** branch on a stable, explicit value instead - an id field, an enum, a sort key
computed from the data itself:
```python
@workflow
async def process(a: Task, b: Task) -> None:
    if a.priority < b.priority:
        ...
```

Using `hash()`/`id()` for something other than a branch (a cache key that is never
compared for control flow, for instance) is not flagged.

---

### LW113: global-mutation

**What:** flags an assignment to a name that a `global` statement in the same function
scope declares (plain, augmented, annotated, or through tuple/starred unpacking).

**Why:** a Python module is imported once per worker process; a plain module-level
variable is not part of the workflow's recorded history and is not synchronized between
the live tick and a later replay (possibly in a different process entirely). Mutating one
from a workflow silently makes the workflow's behavior depend on - and corrupt - whatever
process happens to run it.

**Bad:**
```python
_last_seen: str | None = None

@workflow
async def process(order_id: str) -> None:
    global _last_seen
    _last_seen = order_id
```

**Good:** keep the value in workflow-local state, or return it and let the caller decide
what to do with it:
```python
@workflow
async def process(order_id: str) -> str:
    return order_id
```

Reading a global (without a `global` statement, or with one but no assignment) is not
flagged - only mutation is a determinism problem.

---

### LW199: custom-forbidden-call

**What:** flags any call whose fully-resolved dotted name is listed in
`extend-forbidden-calls` in `pyproject.toml`.

**Why:** a project may have its own known-nondeterministic helpers (a legacy client, an
internal wrapper around a database) that the built-in rules cannot know about by name.

**Example:**
```toml
[tool.loomwork.lint]
extend-forbidden-calls = ["myapp.legacy.fetch_inventory"]
```
```python
@workflow
async def process() -> int:
    return myapp.legacy.fetch_inventory()   # flagged: LW199
```

The matching is by exact dotted path only (as resolved through the file's own imports,
the same way every other rule resolves calls) - there is no prefix or wildcard form for
custom entries.
