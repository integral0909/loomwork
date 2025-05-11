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
