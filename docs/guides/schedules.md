# Schedules

A schedule starts a workflow on a recurring timetable, given as a cron expression or a
fixed interval.

```python
schedule = await client.schedules.create(
    "reports.emea.daily",
    daily_report,
    "emea",                              # workflow arguments
    cron="0 6 * * MON-FRI",
    timezone="Europe/Berlin",
    overlap="skip",
    catchup_window=timedelta(minutes=10),
)
```

Every scheduled run gets a stable workflow id, `<schedule id>@<nominal time>`, and can read
the time it was *scheduled* for, which is the right value to use for "the report for
Monday" even if the run starts late:

```python
@lw.workflow
async def daily_report(region: str) -> None:
    day = lw.info().scheduled_time.date()
    ...
```

## Cron expressions

Five fields (`minute hour day-of-month month day-of-week`), or six with a leading seconds
field.

| Syntax              | Meaning                                  |
| ------------------- | ---------------------------------------- |
| `*/15 * * * *`      | every 15 minutes                         |
| `0 9-17 * * MON-FRI`| on the hour, 9 to 17, weekdays           |
| `0 0 L * *`         | midnight on the last day of each month   |
| `0 0 L-2 * *`       | two days before the last day             |
| `0 12 * * 5L`       | noon on the last Friday of each month    |
| `0 9 * * 1#1`       | 09:00 on the first Monday of each month  |
| `*/20 * * * * *`    | every 20 seconds                         |
| `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly` | the usual macros |

When both day-of-month and day-of-week are restricted, a day matches if **either** does,
as in classic cron.

### Time zones and daylight saving

Expressions describe wall-clock times in `timezone` (UTC by default).

- A time that does not exist on a spring-forward day fires at the equivalent instant
  after the gap: `30 2 * * *` in Europe/Berlin fires at 03:30 local time that day.
- A time that occurs twice on a fall-back day fires once, at its first occurrence.

## Intervals

```python
await client.schedules.create("sync", sync_inventory, interval=timedelta(minutes=10),
                              offset=timedelta(minutes=2))       # :02, :12, :22, ...
```

Intervals are aligned to the Unix epoch plus `offset`, so they fire at predictable times
regardless of when the schedule was created.

## Id policies, versioning and per-run timeouts

`client.schedules.create(...)` accepts the same policies as `start_workflow`/
`start_child`, applied to every run the schedule starts:

```python
await client.schedules.create(
    "billing.monthly_invoice", monthly_invoice, "cust-7",
    cron="0 3 1 * *",
    id_reuse_policy=lw.IdReusePolicy.REJECT_DUPLICATE,
    versioning_policy=lw.VersioningPolicy.PINNED,
    run_timeout=timedelta(hours=2),
    execution_timeout=timedelta(hours=6),
    search_attributes={"kind": "monthly_invoice"},
)
```

| Field | Default | Effect |
| --- | --- | --- |
| `id_reuse_policy` | `ALLOW_DUPLICATE` | See [id reuse and conflict policies](workflows.md#id-reuse-and-conflict-policies); applies when the exact nominal fire time was started before and has since closed (a re-evaluation after a crash, or overlapping `backfill` ranges). |
| `id_conflict_policy` | `FAIL` | See below: what a fire time does when its target workflow id is, unusually, already occupied. |
| `versioning_policy` | resolved from engine configuration | Same override as `start_workflow`/`start_child`; unset, every run the schedule starts resolves its policy exactly as an unspecified top-level start would (see [versioning](versioning.md) and [ADR-0006](../adr/0006-worker-build-versioning.md)). |
| `run_timeout`, `execution_timeout` | none | Bound each run the schedule starts, the same as the identically named `start_workflow` arguments. |
| `search_attributes` | none | Set on every run the schedule starts; validated and encoded once, at `create()` (or `update()`) time. |

A schedule created before these fields existed has none of them in its stored document;
loading it behaves exactly as it always did (`ALLOW_DUPLICATE`/`FAIL`, no override, no
extra timeout or attributes) -- they are additive, like every other field on a stored
schedule.

### `id_conflict_policy` and overlap, together

Scheduled workflow ids embed the nominal fire time (`<schedule id>@<nominal time>`), so
two *different* fire times never collide, and `overlap` already decides whether a fire
time tries to start a run at all given the schedule's own record of what it started
(`skip`, `buffer_one`, `cancel_other`, `terminate_other`; see below). `id_conflict_policy`
only matters for the much narrower case of a fire time that overlap policy *did* decide
to start, finding its own target workflow id already occupied -- which normally cannot
happen, and only does when the exact same fire time is evaluated twice (a crash between
starting the run and recording it, two overlapping `backfill` ranges, or two manual
`trigger()` calls close enough together to share a nominal time):

| Policy | Effect |
| --- | --- |
| `FAIL` (default) | The fire time is skipped and recorded as `already_running`, same as today. |
| `USE_EXISTING` | The schedule adopts the run it found instead of dropping the fire time: `describe()` reports it under `running`/`recent` from here on, as if the schedule had started it. |
| `TERMINATE_EXISTING` | The run it found is terminated and the fire time starts fresh in its place. |

## Overlap policies

What happens when a fire time arrives while the previous run is still running:

| Policy            | Behaviour                                                       |
| ----------------- | --------------------------------------------------------------- |
| `skip` (default)  | Skip this fire time.                                            |
| `buffer_one`      | Start one run as soon as the running one finishes.              |
| `allow_all`       | Start regardless; runs overlap.                                 |
| `cancel_other`    | Ask the running run to cancel, then start the new one.          |
| `terminate_other` | Terminate the running run, then start the new one.              |

Runs that continued as new count as still running.

## Outages and catch-up

If no scheduler ran for a while (a deploy, an outage), fire times that are older than
`catchup_window` are skipped instead of started late. Within the window, only the most
recent fire time starts (all of them with `allow_all`).

## Managing schedules

```python
handle = client.schedules.get("reports.emea.daily")
await handle.pause("quarter-end freeze")
await handle.unpause()                        # does not backfill the paused period
await handle.trigger()                        # start one run now
await handle.backfill(start, end)             # one run per fire time in (start, end]
await handle.update(spec=ScheduleSpec(cron="0 7 * * *", timezone="Europe/Berlin"))  # loomwork.schedules.model
print(await handle.upcoming(5))
state = await handle.describe()               # running runs, recent actions, counters
await handle.delete()                         # runs already started are unaffected
```

`pause_on_failure=True` pauses a schedule automatically when one of its runs fails.
`jitter=timedelta(seconds=30)` spreads starts deterministically, to avoid many
schedules hitting a downstream system in the same second.

## How schedules run

Any worker with `run_schedules=True` (the default) evaluates due schedules. Each
evaluation is a pure decision followed by one atomic, version-checked write that also
creates the new runs, so several workers can evaluate schedules concurrently without
starting a run twice.
