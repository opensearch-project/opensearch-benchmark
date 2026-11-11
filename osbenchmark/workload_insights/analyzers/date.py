# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from osbenchmark.workload_insights.analyzers.base import Analyzer
from osbenchmark.workload_insights.models import DateStats, FieldStats

EPOCH_MILLIS = "epoch_millis"
EPOCH_SECOND = "epoch_second"
ISO_8601 = "strict_date_optional_time"

# strict_date_optional_time: a date (year, year and month or full date), optionally followed by a time (hours, minutes, seconds and
# a fraction of 1 to 9 digits) and an offset. datetime.fromisoformat() cannot be used: before Python 3.11 it accepts only 3 or 6
# fraction digits and offsets with a colon, and OSB supports Python 3.10.
_ISO_8601 = re.compile(
    r"^(?P<year>\d{4})(?:-(?P<month>\d{2})(?:-(?P<day>\d{2}))?)?"
    r"(?:[T ](?P<hour>\d{2})(?::(?P<minute>\d{2})(?::(?P<second>\d{2})(?:[.,](?P<fraction>\d{1,9}))?)?)?)?"
    r"(?P<offset>Z|[+-]\d{2}(?::?\d{2})?)?$")


class DateAnalyzer(Analyzer):
    """Analyzes date and date_nanos fields."""

    def analyze(self, field_type: str, definition: Dict[str, Any], per_doc_values: List[List[Any]]) -> FieldStats:
        mapping_format = definition.get("format")
        numeric_format = self._numeric_format(mapping_format)
        timestamps = []
        inferred_formats = set()
        for values in per_doc_values:
            for value in values:
                parsed = self._parse(value, numeric_format)
                if parsed is None:
                    self.logger.debug("Ignoring unparseable value of type [%s] in date field.", type(value).__name__)
                else:
                    timestamps.append(parsed[0])
                    inferred_formats.add(parsed[1])
        stats = {"format": mapping_format}
        if timestamps:
            stats["min"] = min(timestamps).isoformat()
            stats["max"] = max(timestamps).isoformat()
            stats["granularity"] = self._granularity(timestamps)
            if mapping_format is None and len(inferred_formats) == 1:
                stats["format"] = next(iter(inferred_formats))
        return DateStats(type=field_type, **self.common_stats(per_doc_values), **stats)

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
            tz_offset = timedelta(hours=int(digits[:2]), minutes=int(digits[2:] or 0))
            if offset[0] == "-":
                tz_offset = -tz_offset
        try:
            return datetime(int(parts["year"]), int(parts["month"] or 1), int(parts["day"] or 1), int(parts["hour"] or 0),
                            int(parts["minute"] or 0), int(parts["second"] or 0), microsecond,
                            tzinfo=timezone(tz_offset)).astimezone(timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _granularity(timestamps: List[datetime]) -> str:
        if any(t.microsecond for t in timestamps):
            return "millisecond"
        if any(t.second for t in timestamps):
            return "second"
        if any(t.minute for t in timestamps):
            return "minute"
        if any(t.hour for t in timestamps):
            return "hour"
        return "day"
