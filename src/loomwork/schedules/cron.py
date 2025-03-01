"""Cron expressions: parsing and computing fire times in a time zone.

Supported syntax
================

Five fields (``minute hour day-of-month month day-of-week``) or six with a leading
``second`` field. Each field accepts:

* ``*`` (``?`` is accepted in the day fields, as a synonym);
* a value, a range ``a-b``, and steps ``*/s``, ``a-b/s``, ``a/s``;
* comma-separated lists of the above;
* month names ``JAN`` to ``DEC`` and weekday names ``SUN`` to ``SAT`` (case-insensitive);
  weekdays are ``0`` to ``7`` where both ``0`` and ``7`` are Sunday;
* ``L`` in day-of-month (last day of the month) and ``L-n`` (``n`` days before it);
* ``nL`` in day-of-week (last weekday ``n`` of the month, e.g. ``5L`` = last Friday) and
  ``n#k`` (the ``k``-th weekday ``n``, e.g. ``1#1`` = first Monday).

Macros: ``@yearly`` (``@annually``), ``@monthly``, ``@weekly``, ``@daily``
(``@midnight``) and ``@hourly``.

As in classic cron, when both day-of-month and day-of-week are restricted a day matches
if *either* matches; when one of them is ``*`` only the other applies.

Time zones and daylight saving time
===================================

Expressions describe *wall-clock* times in a time zone (UTC by default):

* A wall time that does not exist (skipped when clocks spring forward) fires at the
  corresponding instant after the gap, e.g. ``30 2 * * *`` in Europe/Berlin fires at
  03:30 local time on the day of the change.
* A wall time that occurs twice (clocks fall back) fires only once, at its first
  occurrence.

Fire times are therefore always strictly increasing.
"""

from __future__ import annotations

import calendar
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

MACROS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}

_MONTHS = {name: i for i, name in enumerate(calendar.month_abbr) if name}
_MONTHS = {k.upper(): v for k, v in _MONTHS.items()}
_WEEKDAYS = {"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6}

# Searching further than this without a match means the expression can never fire
# (for example "0 0 30 2 *", February 30th).
_MAX_SEARCH_YEARS = 8
# Longer than any daylight-saving shift in the tz database (the largest is two hours).
_DST_LOOKBACK = timedelta(hours=2, minutes=30)


class CronError(ValueError):
    """The cron expression is invalid."""


@dataclass(frozen=True, slots=True)
class _FieldSpec:
    name: str
    low: int
    high: int
    names: dict[str, int] | None = None


_SECOND = _FieldSpec("second", 0, 59)
_MINUTE = _FieldSpec("minute", 0, 59)
_HOUR = _FieldSpec("hour", 0, 23)
_DOM = _FieldSpec("day-of-month", 1, 31)
_MONTH = _FieldSpec("month", 1, 12, _MONTHS)
_DOW = _FieldSpec("day-of-week", 0, 7, _WEEKDAYS)

_TOKEN = re.compile(r"^(?P<base>\*|\?|[A-Za-z0-9]+(?:-[A-Za-z0-9]+)?)(?:/(?P<step>\d+))?$")


def _value(spec: _FieldSpec, text: str) -> int:
    upper = text.upper()
    if spec.names and upper in spec.names:
        return spec.names[upper]
    if not text.isdigit():
        raise CronError(f"invalid {spec.name} value {text!r}")
    value = int(text)
    if not spec.low <= value <= spec.high:
        raise CronError(f"{spec.name} value {value} is outside {spec.low}-{spec.high}")
    return value


def _parse_simple(spec: _FieldSpec, text: str) -> set[int]:
    match = _TOKEN.match(text)
    if not match:
        raise CronError(f"invalid {spec.name} field element {text!r}")
    base, step_text = match["base"], match["step"]
    step = int(step_text) if step_text else 1
    if step < 1:
        raise CronError(f"{spec.name} step must be positive")
    if base in {"*", "?"}:
        low, high = spec.low, spec.high
    elif "-" in base:
        first, last = base.split("-", 1)
        low, high = _value(spec, first), _value(spec, last)
        if low > high:
            raise CronError(f"{spec.name} range {base!r} is backwards")
    else:
        low = _value(spec, base)
        high = spec.high if step_text else low
    return set(range(low, high + 1, step))


def _parse_field(spec: _FieldSpec, text: str) -> tuple[frozenset[int], bool]:
    """Parse a plain field; returns (values, restricted)."""
    values: set[int] = set()
    for part in text.split(","):
        if not part:
            raise CronError(f"empty element in {spec.name} field {text!r}")
        values |= _parse_simple(spec, part)
    restricted = text not in {"*", "?"}
    return frozenset(values), restricted


@dataclass(frozen=True, slots=True)
class _DayOfMonth:
    days: frozenset[int]
    last_offsets: frozenset[int]  # "L" is 0, "L-3" is 3
    restricted: bool

    def matches(self, day: date) -> bool:
        if day.day in self.days:
            return True
        if self.last_offsets:
            last = calendar.monthrange(day.year, day.month)[1]
            return (last - day.day) in self.last_offsets
        return False


@dataclass(frozen=True, slots=True)
class _DayOfWeek:
    weekdays: frozenset[int]  # 0 = Sunday
    last_of: frozenset[int]  # "5L": last Friday
    nth: frozenset[tuple[int, int]]  # "1#2": second Monday
    restricted: bool

    def matches(self, day: date) -> bool:
        weekday = (day.weekday() + 1) % 7  # Python: Monday=0 -> cron: Sunday=0
        if weekday in self.weekdays:
            return True
        if weekday in self.last_of:
            last = calendar.monthrange(day.year, day.month)[1]
            if day.day + 7 > last:
                return True
        if self.nth:
            occurrence = (day.day - 1) // 7 + 1
            if (weekday, occurrence) in self.nth:
                return True
        return False


def _parse_dom(text: str) -> _DayOfMonth:
    days: set[int] = set()
    offsets: set[int] = set()
    for part in text.split(","):
        upper = part.upper()
        if upper == "L":
            offsets.add(0)
        elif upper.startswith("L-") and upper[2:].isdigit():
            offset = int(upper[2:])
            if offset > 30:
                raise CronError(f"day-of-month offset {part!r} is too large")
            offsets.add(offset)
        elif part:
            days |= _parse_field(_DOM, part)[0]
        else:
            raise CronError(f"empty element in day-of-month field {text!r}")
    return _DayOfMonth(frozenset(days), frozenset(offsets), text not in {"*", "?"})


def _parse_dow(text: str) -> _DayOfWeek:
    weekdays: set[int] = set()
    last_of: set[int] = set()
    nth: set[tuple[int, int]] = set()
    for part in text.split(","):
        upper = part.upper()
        if "#" in upper:
            day_text, _, k_text = upper.partition("#")
            if not k_text.isdigit() or not 1 <= int(k_text) <= 5:
                raise CronError(f"invalid occurrence in {part!r} (use 1-5)")
            nth.add((_value(_DOW, day_text) % 7, int(k_text)))
        elif upper.endswith("L") and len(upper) > 1:
            last_of.add(_value(_DOW, upper[:-1]) % 7)
        elif part:
            weekdays |= {v % 7 for v in _parse_field(_DOW, part)[0]}
        else:
            raise CronError(f"empty element in day-of-week field {text!r}")
    return _DayOfWeek(
        frozenset(weekdays), frozenset(last_of), frozenset(nth), text not in {"*", "?"}
    )


def _zone(name: str | tzinfo | None) -> tzinfo:
    if name is None:
        return UTC
    if isinstance(name, tzinfo):
        return name
    if name.upper() == "UTC":
        return UTC
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        raise CronError(f"unknown time zone {name!r}") from None


@dataclass(frozen=True, slots=True)
class CronExpression:
    """A parsed cron expression bound to a time zone."""

    source: str
    seconds: frozenset[int]
    minutes: frozenset[int]
    hours: frozenset[int]
    months: frozenset[int]
    dom: _DayOfMonth
    dow: _DayOfWeek
    zone: tzinfo

    @classmethod
    def parse(cls, expression: str, timezone: str | tzinfo | None = None) -> CronExpression:
        text = " ".join(expression.split())
        if not text:
            raise CronError("empty cron expression")
        expanded = MACROS.get(text.lower(), text)
        if expanded.startswith("@"):
            raise CronError(f"unknown macro {text!r}")
        fields = expanded.split(" ")
        if len(fields) == 5:
            fields = ["0", *fields]
        if len(fields) != 6:
            raise CronError(f"expected 5 or 6 fields, got {len(fields)} in {expression!r}")
        second, minute, hour, dom, month, dow = fields
        return cls(
            source=text,
            seconds=_parse_field(_SECOND, second)[0],
            minutes=_parse_field(_MINUTE, minute)[0],
            hours=_parse_field(_HOUR, hour)[0],
            months=_parse_field(_MONTH, month)[0],
            dom=_parse_dom(dom),
            dow=_parse_dow(dow),
            zone=_zone(timezone),
        )

    def __str__(self) -> str:
        return self.source

    # ------------------------------------------------------------------ matching

    def _day_matches(self, day: date) -> bool:
        if day.month not in self.months:
            return False
        if self.dom.restricted and self.dow.restricted:
            return self.dom.matches(day) or self.dow.matches(day)
        if self.dom.restricted:
            return self.dom.matches(day)
        if self.dow.restricted:
            return self.dow.matches(day)
        return True

    def matches(self, moment: datetime) -> bool:
        """Whether ``moment`` (aware) falls exactly on a fire time (to the second)."""
        local = moment.astimezone(self.zone)
        return (
            local.second in self.seconds
            and local.minute in self.minutes
            and local.hour in self.hours
            and self._day_matches(local.date())
        )

    # ------------------------------------------------------------------ searching

    def _wall_times_after(self, start: datetime) -> Iterator[datetime]:
        """Naive wall-clock times matching the expression, strictly after ``start``."""
        seconds = sorted(self.seconds)
        minutes = sorted(self.minutes)
        hours = sorted(self.hours)
        day = start.date()
        limit = date(start.year + _MAX_SEARCH_YEARS, 12, 31)
        first_day = True
        while day <= limit:
            if day.month not in self.months:
                # Jump to the first day of the next month.
                day = (day.replace(day=1) + timedelta(days=32)).replace(day=1)
                first_day = False
                continue
            if self._day_matches(day):
                for hour in hours:
                    if first_day and hour < start.hour:
                        continue
                    for minute in minutes:
                        if first_day and hour == start.hour and minute < start.minute:
                            continue
                        for second in seconds:
                            candidate = datetime(day.year, day.month, day.day, hour, minute, second)
                            if candidate > start:
                                yield candidate
            day += timedelta(days=1)
            first_day = False

    def next_after(self, moment: datetime) -> datetime:
        """The first fire time strictly after ``moment`` (aware), as an aware UTC time.

        Raises:
            CronError: the expression never fires (e.g. February 30th).
        """
        if moment.tzinfo is None:
            raise ValueError("moment must be timezone-aware")
        local_start = moment.astimezone(self.zone).replace(tzinfo=None, fold=0)
        # Near a fall-back transition the wall clock repeats, so wall times *before*
        # the local start may still be in the future; look back far enough to see them.
        # Candidates at or before `moment` are skipped below.
        if self._offset_changes_near(moment):
            local_start -= _DST_LOOKBACK
        for wall in self._wall_times_after(local_start):
            instant = self._resolve(wall)
            if instant > moment:
                return instant
        raise CronError(f"cron expression {self.source!r} never fires")

    def _offset_changes_near(self, moment: datetime) -> bool:
        if self.zone is UTC:
            return False
        before = (moment - _DST_LOOKBACK).astimezone(self.zone).utcoffset()
        after = (moment + _DST_LOOKBACK).astimezone(self.zone).utcoffset()
        return before != after

    def _resolve(self, wall: datetime) -> datetime:
        """Map a wall-clock time to an instant, following the DST rules above.

        ``fold=0`` picks the first occurrence of a repeated wall time, and interprets a
        nonexistent one with the pre-transition offset, which lands on the equivalent
        instant after the gap.
        """
        return wall.replace(tzinfo=self.zone, fold=0).astimezone(UTC)

    def iter_after(self, moment: datetime, limit: int | None = None) -> Iterator[datetime]:
        """Successive fire times after ``moment`` (at most ``limit`` of them)."""
        count = 0
        current = moment
        while limit is None or count < limit:
            current = self.next_after(current)
            yield current
            count += 1

    def fire_times_between(
        self, start: datetime, end: datetime, *, limit: int = 10_000
    ) -> list[datetime]:
        """Fire times in ``(start, end]``, capped at ``limit``."""
        out: list[datetime] = []
        for instant in self.iter_after(start):
            if instant > end or len(out) >= limit:
                break
            out.append(instant)
        return out

    def previous_before(
        self, moment: datetime, *, horizon: timedelta = timedelta(days=366)
    ) -> datetime | None:
        """The last fire time strictly before ``moment`` within ``horizon``, if any."""
        times = self.fire_times_between(moment - horizon, moment - timedelta(microseconds=1))
        return times[-1] if times else None


def parse_cron(expression: str, timezone: str | tzinfo | None = None) -> CronExpression:
    """Parse ``expression`` (see module docs) in ``timezone`` (default UTC)."""
    return CronExpression.parse(expression, timezone)
