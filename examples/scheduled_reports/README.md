# Scheduled reports

Recurring workflow starts on a cron expression, in more than one time zone, with an
overlap policy, pause/unpause and backfill.

## Scenario

One workflow, `generate_report` (in `workflows.py`), serves three schedules created in
`main.py`:

- **`daily-eu`** -- cron `0 6 * * *` in `Europe/Berlin`, a fast report.
- **`weekly-us`** -- cron `0 8 * * MON` in `America/New_York`, a fast report in a
  different time zone, proving the two schedules' local times are each honored
  independently (and correctly crossing daylight-saving boundaries, though this demo
  doesn't run long enough to cross one).
- **`audit-hourly`** -- an *interval* schedule (once an hour) whose report deliberately
  takes three hours to "generate" (`duration_minutes=180`), so it reliably overlaps its
  own next few fire times. With the default `overlap="skip"`, those overlapping fire
  times are skipped rather than queued up or left to run concurrently.

Inside the workflow, `loomwork.info().scheduled_time` is the schedule's *nominal* fire
time -- the date the report is about -- which is what a report actually needs, as
opposed to whatever moment the run happened to actually start at (immediately, if nothing
else is going on; hours later, if it had to wait behind an overlap; or well after the
fact, if it was started retroactively by a backfill).

`main.py` also demonstrates pausing `daily-eu`, letting three days of fire times go by
unstarted, then `unpause()`-ing it (which does not, by itself, catch up the paused
period) and calling `backfill()` to explicitly start a run for each fire time that was
missed.

## Loomwork features used

- `client.schedules.create()` with `cron=` and a `timezone=`, and separately with
  `interval=`.
- `loomwork.info().scheduled_time` and `.schedule_id` inside the workflow.
- `OverlapPolicy.SKIP` (from `loomwork.schedules.model`) for a schedule whose runs can
  take longer than its own interval.
- `ScheduleHandle.pause()` / `.unpause()` / `.trigger()` / `.backfill()` / `.upcoming()`.

## Running it

```console
uv run python -m examples.scheduled_reports.main
```

This demo drives a `loomwork.ManualClock` forward in large steps (an hour or more at a
time -- see `examples.support.advance_until`) instead of waiting in real time for a cron
schedule to actually reach 6am, or for a report to actually take three hours. Because a
schedule's evaluation can then lag its nominal fire time by more *simulated* time than a
continuously-running deployment ever would, `daily-eu` and `weekly-us` are created with a
wider-than-default `catchup_window` so a fire time is never mistaken for one that was
missed during a (fictional) outage; this is unrelated to the overlap policy demonstrated
by `audit-hourly`, which is deliberately given no such allowance. Even simulating more
than a week of cron activity across three schedules, the whole demo finishes in a
fraction of a second.

## Tests

`tests/examples/test_scheduled_reports.py` checks:

- a cron schedule fires at the expected UTC instant for a given time zone;
- `ScheduleHandle.upcoming()` predicts the next fire times correctly;
- pausing, then backfilling, starts exactly the runs the paused window missed;
- an interval schedule whose workflow overruns its own interval skips the fire times it
  overlaps, and resumes starting new runs once the slow run finishes.
