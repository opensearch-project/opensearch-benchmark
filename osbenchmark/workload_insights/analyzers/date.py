# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from osbenchmark.workload_insights.analyzers.base import Analyzer
from osbenchmark.workload_insights.models import DateStats, FieldStats

EPOCH_MILLIS = "epoch_millis"
EPOCH_SECOND = "epoch_second"
ISO_8601 = "strict_date_optional_time"
# the largest offset that java.time.ZoneOffset, and with it OpenSearch, accepts
MAX_OFFSET_MINUTES = 18 * 60

# strict_date_optional_time: a date (year, year and month or full date), optionally followed by a time (hours, minutes, seconds and
# a fraction of 1 to 9 digits) and an offset. datetime.fromisoformat() cannot be used: before Python 3.11 it accepts only 3 or 6
# fraction digits and offsets with a colon, and OSB supports Python 3.10.
# from the coarsest to the finest granularity; any sub-second precision is "millisecond"
GRANULARITIES = ("day", "hour", "minute", "second", "millisecond")

_ISO_8601 = re.compile(
    r"^(?P<year>\d{4})(?:-(?P<month>\d{2})(?:-(?P<day>\d{2}))?)?"
    r"(?:[T ](?P<hour>\d{2})(?::(?P<minute>\d{2})(?::(?P<second>\d{2})(?:[.,](?P<fraction>\d{1,9}))?)?)?)?"
    r"(?P<offset>Z|[+-]\d{2}(?::?\d{2})?)?$")


class DateAnalyzer(Analyzer):
    """Analyzes date and date_nanos fields."""

    def __init__(self, field_type: str = "", definition: Optional[Dict[str, Any]] = None, capture_values: bool = False):
        super().__init__(field_type, definition, capture_values)
        self._mapping_format = self.definition.get("format")
        self._numeric_format_of_mapping = self._numeric_format(self._mapping_format)
        self._min: Optional[datetime] = None
        self._max: Optional[datetime] = None
        self._inferred_formats: Set[str] = set()
        # the index in GRANULARITIES of the finest granularity of the values
        self._granularity_index = 0

    def _add_values(self, values: List[Any]) -> None:
        for value in values:
            parsed = self._parse(value, self._numeric_format_of_mapping)
            if parsed is None:
                self.logger.debug("Ignoring unparseable value of type [%s] in date field.", type(value).__name__)
                continue
            timestamp, inferred_format = parsed
            self._min = timestamp if self._min is None else min(self._min, timestamp)
            self._max = timestamp if self._max is None else max(self._max, timestamp)
            self._inferred_formats.add(inferred_format)
            self._granularity_index = max(self._granularity_index, self._granularity_of(timestamp))

    def _merge_values(self, other: "DateAnalyzer") -> None:  # pylint: disable=arguments-renamed
        # pylint: disable=protected-access
        if other._min is None:
            return
        self._min = other._min if self._min is None else min(self._min, other._min)
        self._max = other._max if self._max is None else max(self._max, other._max)
        self._inferred_formats |= other._inferred_formats
        self._granularity_index = max(self._granularity_index, other._granularity_index)

    def result(self) -> FieldStats:
        stats = {"format": self._mapping_format}
        if self._min is not None:
            stats["min"] = self._min.isoformat()
            stats["max"] = self._max.isoformat()
            stats["granularity"] = GRANULARITIES[self._granularity_index]
            if self._mapping_format is None and len(self._inferred_formats) == 1:
                stats["format"] = next(iter(self._inferred_formats))
        return DateStats(type=self.field_type, **self._common_stats(), **stats)

    @staticmethod
    def _numeric_format(mapping_format: Optional[str]) -> str:
        """
        :return: The format that applies to numeric values: the first epoch format of the mapping, epoch_millis by default.
        """
        for candidate in (mapping_format or "").split("||"):
            if candidate.strip() in (EPOCH_SECOND, EPOCH_MILLIS):
                return candidate.strip()
        return EPOCH_MILLIS

    @staticmethod
    def _parse(value: Any, numeric_format: str = EPOCH_MILLIS) -> Optional[Tuple[datetime, str]]:
        """
        :return: A tuple of the timestamp in UTC and the inferred format or None if the value cannot be parsed.
        """
        if isinstance(value, bool):
            return None
        try:
            if isinstance(value, (int, float)):
                seconds = value if numeric_format == EPOCH_SECOND else value / 1000.0
                return datetime.fromtimestamp(seconds, tz=timezone.utc), numeric_format
            if isinstance(value, str):
                parsed = DateAnalyzer._parse_iso_8601(value.strip())
                return (parsed, ISO_8601) if parsed else None
        except (ValueError, OverflowError, OSError):
            return None
        return None

    @staticmethod
    def _parse_iso_8601(value: str) -> Optional[datetime]:
        match = _ISO_8601.match(value)
        if not match:
            return None
        parts = match.groupdict()
        # datetime has microsecond precision: finer fractions (date_nanos) are truncated
        microsecond = int(((parts["fraction"] or "") + "000000")[:6])
        offset = parts["offset"]
        if offset is None or offset == "Z":
            tz_offset = timedelta(0)
        else:
            digits = offset[1:].replace(":", "")
            hours, minutes = int(digits[:2]), int(digits[2:] or 0)
            if minutes > 59 or hours * 60 + minutes > MAX_OFFSET_MINUTES:
                return None
            tz_offset = timedelta(hours=hours, minutes=minutes)
            if offset[0] == "-":
                tz_offset = -tz_offset
        try:
            return datetime(int(parts["year"]), int(parts["month"] or 1), int(parts["day"] or 1), int(parts["hour"] or 0),
                            int(parts["minute"] or 0), int(parts["second"] or 0), microsecond,
                            tzinfo=timezone(tz_offset)).astimezone(timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _granularity_of(timestamp: datetime) -> int:
        """
        :return: The index in GRANULARITIES of the finest unit in which the timestamp differs from midnight.
        """
        if timestamp.microsecond:
            return 4
        if timestamp.second:
            return 3
        if timestamp.minute:
            return 2
        if timestamp.hour:
            return 1
        return 0
