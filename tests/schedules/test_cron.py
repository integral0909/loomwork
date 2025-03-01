"""Tests for cron parsing and fire-time computation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from loomwork.schedules.cron import CronError, parse_cron


def utc(
    year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0
) -> datetime:
    return datetime(year, month, day, hour, minute, second, tzinfo=UTC)


def fires(expr: str, start: datetime, count: int, tz: str | None = None) -> list[datetime]:
    return list(parse_cron(expr, tz).iter_after(start, count))


class TestParsing:
    @pytest.mark.parametrize(
        ("expr", "field", "expected"),
        [
            ("*/15 * * * *", "minutes", {0, 15, 30, 45}),
            ("5-10/2 * * * *", "minutes", {5, 7, 9}),
            ("0 9-17 * * *", "hours", set(range(9, 18))),
            ("0 0 * JAN,jul *", "months", {1, 7}),
            ("0 0 * * *", "seconds", {0}),
            ("*/20 0 0 * * *", "seconds", {0, 20, 40}),
            ("0 0 * 3/4 *", "months", {3, 7, 11}),
        ],
    )
    def test_field_values(self, expr: str, field: str, expected: set[int]) -> None:
        assert set(getattr(parse_cron(expr), field)) == expected

    def test_weekday_names_and_sunday_as_seven(self) -> None:
        assert parse_cron("0 0 * * MON-FRI").dow.weekdays == frozenset({1, 2, 3, 4, 5})
        assert parse_cron("0 0 * * 7").dow.weekdays == frozenset({0})
        assert parse_cron("0 0 * * sun,SAT").dow.weekdays == frozenset({0, 6})

    @pytest.mark.parametrize(
        "macro", ["@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly"]
    )
    def test_macros(self, macro: str) -> None:
        assert parse_cron(macro).seconds == frozenset({0})

    def test_whitespace_is_normalised(self) -> None:
        assert str(parse_cron("  0   12 * *  * ")) == "0 12 * * *"

    @pytest.mark.parametrize(
        "expr",
        [
            "",
            "* * * *",
            "* * * * * * *",
            "60 * * * *",
            "* 24 * * *",
            "* * 0 * *",
            "* * * 13 *",
            "* * * * 8",
            "5-1 * * * *",
            "*/0 * * * *",
            "a * * * *",
            "1,,2 * * * *",
            "* * * FOO *",
            "* * * * 1#6",
            "* * L-40 * *",
            "@fortnightly",
        ],
    )
    def test_invalid(self, expr: str) -> None:
        with pytest.raises(CronError):
            parse_cron(expr)

    def test_unknown_zone(self) -> None:
        with pytest.raises(CronError, match="time zone"):
            parse_cron("* * * * *", "Mars/Olympus_Mons")


class TestNextFire:
    def test_every_minute(self) -> None:
        assert fires("* * * * *", utc(2024, 1, 1, 0, 0, 30), 2) == [
            utc(2024, 1, 1, 0, 1),
            utc(2024, 1, 1, 0, 2),
        ]

    def test_strictly_after(self) -> None:
        assert fires("0 12 * * *", utc(2024, 1, 1, 12), 1) == [utc(2024, 1, 2, 12)]

    def test_seconds_field(self) -> None:
        assert fires("*/20 * * * * *", utc(2024, 1, 1, 0, 0, 5), 3) == [
            utc(2024, 1, 1, 0, 0, 20),
            utc(2024, 1, 1, 0, 0, 40),
            utc(2024, 1, 1, 0, 1, 0),
        ]

    def test_month_rollover_and_leap_day(self) -> None:
        assert fires("0 0 29 2 *", utc(2024, 3, 1), 1) == [utc(2028, 2, 29)]
        assert fires("0 0 31 * *", utc(2024, 4, 1), 2) == [utc(2024, 5, 31), utc(2024, 7, 31)]

    def test_last_day_of_month(self) -> None:
        assert fires("0 0 L * *", utc(2024, 1, 31, 1), 3) == [
            utc(2024, 2, 29),
            utc(2024, 3, 31),
            utc(2024, 4, 30),
        ]
        assert fires("0 0 L-1 * *", utc(2024, 2, 1), 1) == [utc(2024, 2, 28)]

    def test_nth_and_last_weekday(self) -> None:
        # First Monday of each month.
        assert fires("0 9 * * 1#1", utc(2024, 1, 1, 10), 2) == [
            utc(2024, 2, 5, 9),
            utc(2024, 3, 4, 9),
        ]
        # Last Friday.
        assert fires("0 17 * * 5L", utc(2024, 3, 1), 2) == [
            utc(2024, 3, 29, 17),
            utc(2024, 4, 26, 17),
        ]

    def test_dom_or_dow_when_both_restricted(self) -> None:
        # The 13th of the month OR any Friday.
        times = fires("0 0 13 * FRI", utc(2024, 9, 1), 4)
        assert times == [utc(2024, 9, 6), utc(2024, 9, 13), utc(2024, 9, 20), utc(2024, 9, 27)]

    def test_only_dow_restricted(self) -> None:
        times = fires("0 8 * * MON-FRI", utc(2024, 9, 27, 9), 2)  # a Friday after 08:00
        assert times == [utc(2024, 9, 30, 8), utc(2024, 10, 1, 8)]

    def test_never_fires(self) -> None:
        with pytest.raises(CronError, match="never fires"):
            parse_cron("0 0 30 2 *").next_after(utc(2024, 1, 1))

    def test_naive_moment_rejected(self) -> None:
        with pytest.raises(ValueError, match="aware"):
            parse_cron("* * * * *").next_after(datetime(2024, 1, 1))

    def test_matches(self) -> None:
        expr = parse_cron("30 9 * * MON", "Europe/Berlin")
        assert expr.matches(datetime(2024, 9, 30, 9, 30, tzinfo=ZoneInfo("Europe/Berlin")))
        assert not expr.matches(datetime(2024, 9, 30, 9, 31, tzinfo=ZoneInfo("Europe/Berlin")))

    def test_between_and_previous(self) -> None:
        expr = parse_cron("0 * * * *")
        assert expr.fire_times_between(utc(2024, 1, 1), utc(2024, 1, 1, 3)) == [
            utc(2024, 1, 1, 1),
            utc(2024, 1, 1, 2),
            utc(2024, 1, 1, 3),
        ]
        assert expr.previous_before(utc(2024, 1, 1, 3, 30)) == utc(2024, 1, 1, 3)
        assert (
            parse_cron("0 0 1 1 *").previous_before(utc(2024, 6, 1), horizon=timedelta(days=30))
            is None
        )


class TestTimeZones:
    def test_local_wall_time(self) -> None:
        # 09:00 in New York is 13:00 UTC in summer and 14:00 UTC in winter.
        times = fires("0 9 * * *", utc(2024, 11, 2, 12), 2, "America/New_York")
        assert times == [utc(2024, 11, 2, 13), utc(2024, 11, 3, 14)]

    def test_spring_forward_gap_fires_after_the_gap(self) -> None:
        # Berlin skips 02:00-03:00 on 2024-03-31.
        times = fires("30 2 * * *", utc(2024, 3, 30, 12), 2, "Europe/Berlin")
        assert times[0] == utc(2024, 3, 31, 1, 30)  # 03:30 CEST
        assert times[1] == utc(2024, 4, 1, 0, 30)  # 02:30 CEST

    def test_fall_back_fires_once(self) -> None:
        # Berlin repeats 02:00-03:00 on 2024-10-27; 02:30 must fire once.
        times = fires("30 2 * * *", utc(2024, 10, 26, 12), 2, "Europe/Berlin")
        assert times == [utc(2024, 10, 27, 0, 30), utc(2024, 10, 28, 1, 30)]

    def test_hourly_across_fall_back(self) -> None:
        # Start at 01:30 CEST. Then: 02:00 CEST, (02:00 CET is the same wall time and is
        # skipped), 03:00 CET, 04:00 CET, 05:00 CET.
        times = fires("0 * * * *", utc(2024, 10, 26, 23, 30), 4, "Europe/Berlin")
        assert times == [
            utc(2024, 10, 27, 0),
            utc(2024, 10, 27, 2),
            utc(2024, 10, 27, 3),
            utc(2024, 10, 27, 4),
        ]

    def test_hourly_across_spring_forward(self) -> None:
        times = fires("0 * * * *", utc(2024, 3, 31, 0, 30), 3, "Europe/Berlin")
        # 02:00 does not exist; its equivalent instant equals 03:00 CEST, fired once.
        assert times == [utc(2024, 3, 31, 1), utc(2024, 3, 31, 2), utc(2024, 3, 31, 3)]


@settings(max_examples=200, deadline=None)
@given(
    minute=st.sets(st.integers(0, 59), min_size=1, max_size=4),
    hour=st.sets(st.integers(0, 23), min_size=1, max_size=3),
    zone=st.sampled_from(
        ["UTC", "Europe/Berlin", "America/New_York", "Australia/Lord_Howe", "Asia/Kolkata"]
    ),
    start=st.datetimes(
        min_value=datetime(2020, 1, 1), max_value=datetime(2030, 1, 1), timezones=st.just(UTC)
    ),
)
def test_fire_times_are_increasing_and_match(
    minute: set[int], hour: set[int], zone: str, start: datetime
) -> None:
    expr = parse_cron(
        f"{','.join(map(str, sorted(minute)))} {','.join(map(str, sorted(hour)))} * * *", zone
    )
    previous = start
    for instant in expr.iter_after(start, 5):
        assert instant > previous
        local = instant.astimezone(expr.zone)
        # Either an exact match, or the shifted image of a nonexistent wall time.
        assert expr.matches(instant) or local.dst() != (local - timedelta(hours=3)).dst()
        previous = instant
