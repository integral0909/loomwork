# The `loomwork` command

Installing Loomwork puts a `loomwork` command on your path (also available as
`python -m loomwork.cli`). It runs workers and the server, starts and inspects workflow
runs, manages schedules, tokens and stores, and checks new workflow code before a deploy.

```
loomwork [--json] [--log-level LEVEL] COMMAND ...
```

| Command | What it does |
| ------- | ------------ |
| [`workflow`](#loomwork-workflow) (`wf`) | start, list, describe, signal, query, update, cancel and terminate runs |
| [`schedule`](#loomwork-schedule) (`sched`) | create, pause, trigger, backfill, update and delete schedules |
| [`worker`](#loomwork-worker) | run a worker for your workflow code |
| [`server`](#loomwork-server) | run the HTTP server |
| [`store`](#loomwork-store) | migrate, inspect and prune a store |
| [`doctor`](#loomwork-doctor) | check a deployment's health and print a pass/warn/fail report |
| [`archive`](#loomwork-archive) | write closed runs to long-term storage, and read them back |
| [`replay`](#loomwork-replay) | step through a history tick by tick against workflow code |
| [`compat`](#loomwork-compat) | replay recorded histories with new workflow code |
| [`lint`](#loomwork-lint) | static determinism checks |
| [`cron`](#loomwork-cron) | print the next fire times of a cron expression |
| [`token`](#loomwork-token) | manage the server's bearer tokens |

## Conventions

**Output.** Tables and `key: value` blocks go to stdout; remarks such as "started ..." or
"no runs found" go to stderr, so stdout stays clean for pipes. `--json` (accepted anywhere
on the line) switches stdout to JSON with the full records instead of the table columns:

```
loomwork workflow list --status failed --json | jq -r '.[].workflow_id'
```

**Exit codes.**

| Code | Meaning |
| ---- | ------- |
| 0 | success |
| 1 | the command ran, and the answer is "no": a run failed, a compatibility check found broken runs, a lint found problems, a cancel found the run already closed |
| 2 | the command line was wrong: unknown option, invalid JSON argument, invalid duration or query |
| 3 | the workflow, run or schedule does not exist |
| 4 | the store or server could not be reached |
| 130 | interrupted |

**Durations** are seconds (`90`) or units: `250ms`, `30s`, `5m`, `2h`, `7d`, `1w`, and
combinations such as `1h30m`.

**Times** are ISO 8601 (`2024-05-01T12:00:00Z`; a time without an offset is UTC), `now`,
or relative to now: `-2h`, `+30m`.

**Workflow arguments** are JSON values, one per argument. Quote them for the shell:
`'"text"'`, `42`, `'{"id": 7}'`. `@file.json` reads a value from a file and `@-` from
stdin. With `--raw`, arguments are passed as plain strings instead.

## Connecting

Commands that touch workflows need a store. Flags win over environment variables:

| Flag | Environment | |
| ---- | ----------- | - |
| `--store URL` | `LOOMWORK_STORE_URL` | `sqlite:///path/to.db`, `postgresql://...`, `memory://` |
| `--server URL` | `LOOMWORK_SERVER_URL` | a [Loomwork server](server.md) |
| `--token-file PATH` | `LOOMWORK_TOKEN_FILE` | file holding the server bearer token |
| | `LOOMWORK_TOKEN` | the token itself |
| `--namespace NAME` | `LOOMWORK_NAMESPACE` | server namespace (default `default`) |
| `--attr-type NAME=TYPE` | `LOOMWORK_SEARCH_ATTRIBUTES` | search attribute types for queries (`tier=string,amount=float`) |
| `--timeout SECONDS` | | server request timeout (default 30) |

There is deliberately no `--token` flag: command lines are visible in the process list and
end up in shell history.

`loomwork workflow` and `loomwork schedule` pick which of the server's two APIs to use
based on the token's role, so an operator never needs to be handed worker credentials
just to run the CLI: a `worker` or `admin` token uses the same worker API as a remote
worker (`HttpStore`), unchanged; an `operator` or `reader` token -- which cannot use the
worker API at all -- uses the plain-JSON control API instead
([server mode](server.md#two-apis-one-server)), automatically and without a flag to
choose it. Both give identical output. A few commands always need the worker API
regardless of token, because they need more than the control API can answer:
`workflow query` (needs the workflow code, loaded with `--app`), `workflow steps`,
`worker`, `store`, `replay` and `compat`; using one of these with an operator/reader
token gets a clear "does not grant" error rather than a confusing one.

Passwords in store URLs are never printed; error messages show the URL with the password
replaced.

## `loomwork workflow`

```
loomwork workflow start NAME [ARG ...] [--id ID] [--attr KEY=VALUE ...]
                        [--run-timeout D] [--execution-timeout D] [--retry-attempts N]
                        [--id-reuse-policy POLICY] [--id-conflict-policy POLICY]
                        [--wait [--wait-timeout D]] [--raw]
loomwork workflow list [--workflow NAME] [--status STATUS] [-q QUERY] [--limit N]
loomwork workflow describe WORKFLOW_ID [--run RUN_ID]
loomwork workflow history  WORKFLOW_ID [--run RUN_ID] [--kind KIND ...] [--export | -o FILE]
loomwork workflow result   WORKFLOW_ID [--run RUN_ID] [--wait [--wait-timeout D]]
loomwork workflow signal   WORKFLOW_ID CHANNEL VALUE [--message-id ID] [--raw]
loomwork workflow query    WORKFLOW_ID NAME [ARG ...] --app REF
loomwork workflow update   WORKFLOW_ID NAME [ARG ...] [--update-id ID] [--wait-timeout D]
loomwork workflow cancel   WORKFLOW_ID [--reason TEXT]
loomwork workflow terminate WORKFLOW_ID --reason TEXT
loomwork workflow reset    WORKFLOW_ID [--run RUN_ID] (--to POINT | --after-tick N)
                        --reason TEXT [--wait [--wait-timeout D]]
loomwork workflow batch signal|cancel|terminate --query Q [...]
                        [--page-size N] [--concurrency N] [--dry-run]
                        [--max-runs N] [--yes]
loomwork workflow steps    WORKFLOW_ID          # pending step jobs, attempts, last failure
loomwork workflow children WORKFLOW_ID
loomwork workflow graph    WORKFLOW_ID [--run RUN_ID] --format mermaid|dot|text
```

A workflow id refers to its latest run; `--run` picks a specific one (and can be used
without the id).

`--id-reuse-policy` (`allow_duplicate`, the default; `allow_duplicate_failed_only`;
`reject_duplicate`) controls whether `--id` may be reused once its latest run has
closed; `--id-conflict-policy` (`fail`, the default; `use_existing`;
`terminate_existing`) controls what happens when it is *currently* running. See
`loomwork.options.IdReusePolicy`/`IdConflictPolicy`.

```
$ loomwork workflow start orders.checkout '{"order_id": "A-17"}' --id order-A-17 --attr tier=gold
started order-A-17 (run 01J1V6Y4ZK2Q8R0M3S7N5T9W1X)

$ loomwork workflow describe order-A-17
workflow id:    order-A-17
run id:         01J1V6Y4ZK2Q8R0M3S7N5T9W1X
workflow:       orders.checkout
status:         running
created:        2024-06-01T09:12:44Z
closed:         -
history length: 9
tick pending:   no
attr.tier:      gold

$ loomwork workflow list -q "status = 'failed' AND attr.tier = 'gold'" --attr-type tier=string
```

`result` prints the result as JSON and exits 0, or prints the failure and exits 1. With
`--wait` it waits for a running run to close; `start --wait` starts a run and waits in one
step, which makes the CLI usable from scripts that need the outcome.

`history --export` (or `-o FILE`) writes the portable JSON document that
[`compat --history`](#loomwork-compat) and the [testkit](testing.md) read, which is the
way to reproduce a production problem locally.

`query` replays the run in the CLI process, so it needs the workflow code (`--app`).

`graph` renders the run's steps, timers, children and waits as a diagram; see
[debugging](debugging.md#history-graphs).

`reset` truncates a run's history at a tick boundary and starts a new run of the same
workflow id from there, printing the new run's id; see
[recovering and operating on running fleets](operations/recovery.md#resetting-a-run) for
what a reset does and does not change. `--to first-tick` replays from scratch;
`--to before-last-failure` keeps everything except the run's last tick (the common case
after a bug caused a bad decision); `--after-tick N` keeps exactly the first `N`
completed ticks. `--wait` waits for the *new* run's result, like `start --wait`.

```
$ loomwork workflow reset order-A-17 --to before-last-failure --reason "fixed in deploy 214"
reset to order-A-17 (run 01J1V808Z2Q8R0M3S7N5T9W1X)
```

`batch signal|cancel|terminate` applies one operation to every run a
[visibility query](visibility-queries.md) matches (`--query`/`-q`; a plain filter, no
`ORDER BY` or `LIMIT` of its own -- batch pages through matches itself). `--dry-run`
lists what the query matches without acting on it. Since a broad query can match far
more runs than intended, the command refuses to act on more than `--max-runs` (default
1000) without `--yes`; pass it once you have checked the dry run, narrow `--query`, or
raise `--max-runs`. `--page-size` and `--concurrency` control how many runs are fetched
per query round trip and how many operations are in flight at once (see
[batch operations](operations/recovery.md#batch-operations)). Prints the resulting
`BatchResult` summary (`matched`, `succeeded`, `skipped_closed`, and any per-run
`failed` errors) and exits 1 if any run failed:

```
$ loomwork workflow batch cancel -q "status = 'running' AND attr.region = 'eu-west'" --dry-run
...
12 run(s) match; dry run, nothing was done
$ loomwork workflow batch cancel -q "status = 'running' AND attr.region = 'eu-west'" --yes
matched 12, succeeded 12, skipped (already closed) 0, failed 0
```

## `loomwork schedule`

```
loomwork schedule create ID WORKFLOW [ARG ...] (--cron EXPR [--tz ZONE] | --every D)
                         [--start-at T] [--end-at T] [--jitter D] [--overlap POLICY]
                         [--catchup-window D] [--pause-on-failure] [--paused] [--note TEXT]
loomwork schedule list
loomwork schedule describe ID
loomwork schedule pause ID [--note TEXT] / unpause ID [--note TEXT]
loomwork schedule trigger ID                     # start one run now
loomwork schedule backfill ID --from T [--to T]  # start runs for past fire times
loomwork schedule upcoming ID [-n COUNT]
loomwork schedule update ID [--cron EXPR | --every D] [--tz ZONE] [--jitter D]
                         [--overlap POLICY] [--catchup-window D]
                         [--pause-on-failure | --no-pause-on-failure] [--note TEXT]
loomwork schedule delete ID                      # runs it started keep running
```

Overlap policies: `skip`, `buffer_one`, `allow_all`, `cancel_other`, `terminate_other`
(see [schedules](guides/schedules.md)).

```
$ loomwork schedule create nightly-report reports.nightly --cron "0 3 * * *" --tz Europe/Stockholm
created schedule nightly-report; next fire 2024-06-02 01:00:00+00:00
$ loomwork schedule backfill nightly-report --from -3d
backfill started 3 run(s)
```

## `loomwork worker`

```
loomwork worker --app REF [--worker-id ID]
                [--workflow-concurrency N] [--step-concurrency N]
                [--queue NAME ...] [--queue-limit QUEUE:rate=R,burst=B,concurrency=C ...]
                [--no-timers] [--no-outbox] [--no-schedules] [--shutdown-grace D]
                [--metrics-port PORT [--metrics-host HOST]]
```

Runs until `SIGINT` or `SIGTERM`, then stops claiming work and gives in-flight ticks and
step attempts `--shutdown-grace` (default 10s) to finish; attempts still running after
that are handed back so another worker restarts them at once. A second signal exits
immediately.

`--app` names your workflow code:

| Reference | Loads |
| --------- | ----- |
| `myapp.workflows` | every `@workflow` and `@step` at the top level of the module |
| `myapp.workflows:app` | a `Loomwork` app (its registry *and* its converter, including payload codecs) |
| `myapp.workflows:registry` | a `Registry`, or a list of definitions |
| `myapp.workflows:build` | a function returning any of the above |
| `path/to/file.py[:name]` | the same, for a file that is not on the import path |

The current directory is importable, as with `python -m`. When payloads are encrypted,
reference the `Loomwork` app so the worker uses the same codec chain.

`--metrics-port` serves Prometheus metrics at `/metrics` and a readiness probe at
`/healthz` (see [monitoring](operations/monitoring.md)). It binds to `127.0.0.1` unless
`--metrics-host` says otherwise.

## `loomwork server`

```
loomwork server --config server.toml [--host HOST] [--port PORT]
```

See [server mode](server.md) for the configuration file.

## `loomwork store`

```
loomwork store migrate                     # apply pending migrations, print the schema version
loomwork store info                        # schema version, running/failed runs, outbox backlog
loomwork store prune --older-than 30d [--dry-run] [--batch N] [--once]
```

`prune` deletes closed runs (records, histories and search attributes) in batches, one
transaction per batch, until none older than the cutoff remain.

## `loomwork doctor`

```
loomwork doctor [--tick-failure-threshold N] [--timer-overdue D] [--step-stall D]
                [--schedule-overdue D] [--outbox-backlog-threshold N]
                [--history-warning-fraction F] [--queue NAME ...] [--limit N]
```

Checks a store or server and prints one `[PASS]`/`[WARN]`/`[FAIL]` line per check, with a
remediation hint under any that did not pass. Exits 1 if any check failed -- start here
when a deployment looks unhealthy (see
[troubleshooting](operations/troubleshooting.md#start-with-loomwork-doctor)):

```
$ loomwork doctor --store sqlite:///loomwork.db
[PASS] schema            schema at version 9, up to date
[PASS] outbox backlog    empty
[FAIL] tick failures     1 run(s) with >= 3 consecutive tick failures: order-42 (5x: NondeterminismError: ...)
       -> see `loomwork workflow describe`/`history`; a NondeterminismError needs a code fix or loomwork.patched(), see docs/guides/versioning.md
[PASS] step job backlog  no ready step jobs waiting
[PASS] timers            no outstanding timers
[PASS] schedules         no schedule is overdue
[PASS] history size      no run is near its history limit
```

Checks: store reachability and schema version (against the newest migration this
release knows); outbox backlog; runs whose ticks keep failing (`--tick-failure-threshold`
consecutive failures); the oldest unclaimed, ready-to-run step job, store-wide and (with
`--queue`, repeatable) for specific queues, against `--step-stall`; the next timer's
overdue time against `--timer-overdue`; schedules whose next fire time is overdue by
`--schedule-overdue`; and running runs approaching the history size limit
(`--history-warning-fraction` of it). `--server` adds two checks a local `--store`
connection cannot make: clock skew between this machine and the server, and the token's
own roles and namespaces. Every query is capped by `--limit` (default 100), so `doctor`
is safe to run against a large production store. `--json` prints
`{"ok": bool, "findings": [{"check", "status", "message", "hint"}, ...]}` for monitoring
integrations.

## `loomwork archive`

```
loomwork archive create --older-than 30d --out DIR [--query Q] [--prune] [--segment-size SIZE]
loomwork archive verify DIR
loomwork archive list DIR [--workflow NAME]
loomwork archive show DIR WORKFLOW_ID|--run RUN_ID [--export FILE]
```

Writes closed runs older than a cutoff to a directory of gzip-compressed JSON Lines
segments plus a manifest, and reads them back streaming, without a store. `--query`
narrows the selection with a [visibility query](visibility-queries.md) filter (no
`ORDER BY` or `LIMIT`); `--prune` deletes the archived runs from the store once the
archive is written, fsynced and independently re-verified, and cannot be combined with
`--query` (it would delete runs the filtered archive never wrote). `--segment-size`
(default `64MB`) controls when a segment rotates. `verify` recomputes every segment's
checksum and reports the precise location of any corruption or truncation; `list` and
`show` stream the archive. See [archiving](operations/archiving.md) for the format and
its crash-safety guarantees.

## `loomwork replay`

```
loomwork replay (--history FILE | --archive DIR --run RUN_ID | WORKFLOW_ID [--run RUN_ID])
                --app REF [--until-tick N] [--query NAME]
```

Replays a history (from a run, an exported history file, or an [archive](operations/archiving.md))
tick by tick against the code in `--app`, printing what each tick consumed and produced.
Exits 1 if the code diverges from the recorded decisions, with a precise explanation of
where and why. `--until-tick N` stops early; `--query NAME` then prints that
zero-argument query's result at the stopping point. See
[debugging](debugging.md#the-replay-debugger).

## `loomwork compat`

```
loomwork compat --app REF [--workflow NAME ...] [--running-limit N] [--closed-limit N] [--all]
loomwork compat --app REF --history FILE [--history FILE ...]
```

Replays recent histories with the code in `--app`: every running run (up to
`--running-limit`) and a sample of recently closed runs per workflow. Prints the runs that
would fail with a nondeterminism error and exits 1 if there are any, so it can gate a
deploy pipeline. Runs of workflows that `--app` does not define are counted and skipped.
See [versioning](guides/versioning.md).

## `loomwork lint`

Everything after `lint` is passed to the linter unchanged; `loomwork lint --help` shows its
options. See [lint rules](lint-rules.md).

## `loomwork cron`

```
$ loomwork cron "30 2 * * *" --tz Europe/Berlin -n 3 --after 2024-03-30
2024-03-30T02:30:00+01:00
2024-03-31T03:30:00+02:00
2024-04-01T02:30:00+02:00
```

Useful for checking an expression before putting it in a schedule, including what happens
around daylight saving changes.

## `loomwork token`

```
loomwork token create ID --role ROLE [--role ROLE ...] [--namespace NS ...]
                      [--description TEXT] [--replace] [-o FILE] [-f TOKENS_FILE]
loomwork token list   [-f TOKENS_FILE]
loomwork token revoke ID [ID ...] [-f TOKENS_FILE]
loomwork token verify [-f TOKENS_FILE] < token.txt
```

The tokens file defaults to `$LOOMWORK_TOKENS_FILE` or `./tokens.json`. `create` prints the
token once (or writes it to `-o FILE` with owner-only permissions); only a salted hash is
stored. `--replace` rotates an existing token. A running server picks up changes within a
second. `verify` reads the token from stdin so it does not appear in the process list.
