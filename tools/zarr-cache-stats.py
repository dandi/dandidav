#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///

"""
This script takes as arguments one or more `dandidav` Heroku logs (assumed to
be consecutive) and outputs various statistics related to the Zarr
manifest cache.
"""

from __future__ import annotations
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import json
import logging
from pathlib import Path
import re
import statistics
import sys
import textwrap

log = logging.getLogger(__name__)


@dataclass
class HerokuLog:
    timestamp: datetime
    target: str
    message: str
    line: str


@dataclass
class Events:
    misses: list[Miss] = field(default_factory=list)
    killed_misses: list[KilledMiss] = field(default_factory=list)
    hits: list[Hit] = field(default_factory=list)
    evictions: list[Eviction] = field(default_factory=list)


@dataclass(frozen=True)
class MissKey:
    span_id: str
    manifest_path: str


@dataclass
class MissMoment:
    timestamp: datetime
    approx_cache_len: int


@dataclass
class Miss:
    manifest_path: str
    start: MissMoment
    end: MissMoment
    #: Number of other misses in progress as of the end of this miss
    parallel: int


@dataclass
class KilledMiss:
    manifest_path: str
    start: MissMoment
    killed_at: datetime


@dataclass
class Hit:
    manifest_path: str
    timestamp: datetime
    # `last_access` is `None` if there was no previous access, presumably
    # because such occurred prior to the start of the given logs.
    last_access: datetime | None
    approx_cache_len: int
    #: The zero-based index of this cache entry among all entries when ordered
    #: from most-recently accessed (hit or missed) to least-recently accessed
    #: as of just before this hit, i.e., the number of other entries in the
    #: cache that were accessed since the last time this entry was accessed
    recency_index: int


@dataclass
class Eviction:
    manifest_path: str
    timestamp: datetime
    cause: RemovalCause


class RemovalCause(Enum):
    # <https://docs.rs/moka/latest/moka/notification/enum.RemovalCause.html>
    EXPIRED = "Expired"
    EXPLICIT = "Explicit"
    REPLACED = "Replaced"
    SIZE = "Size"


@dataclass
class Statistics:
    qty: int
    mean: float
    stddev: float
    minimum: float
    q1: float
    median: float
    q3: float
    maximum: float

    @classmethod
    def for_values(cls, values: Iterable[float]) -> Statistics:
        vals = list(values)
        qty = len(vals)
        minimum = min(vals)
        maximum = max(vals)
        mean = statistics.fmean(vals)
        stddev = statistics.stdev(vals, xbar=mean)
        median = statistics.median(vals)
        q1, _, q3 = statistics.quantiles(vals)
        return cls(
            qty=qty,
            mean=mean,
            stddev=stddev,
            minimum=minimum,
            q1=q1,
            median=median,
            q3=q3,
            maximum=maximum,
        )

    def __str__(self) -> str:
        return (
            f"Qty: {self.qty}\n"
            f"Min: {self.minimum}\n"
            f"Q1:  {self.q1}\n"
            f"Med: {self.median}\n"
            f"Q3:  {self.q3}\n"
            f"Max: {self.maximum}\n"
            f"Avg: {self.mean}\n"
            f"Stddev: {self.stddev}"
        )


def main() -> None:
    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(message)s",
        datefmt="%H:%M:%S",
        level=logging.DEBUG,
    )
    events = process_logs(map(Path, sys.argv[1:]))
    summarize(events)


def iterlogs(filepaths: Iterable[Path]) -> Iterator[HerokuLog]:
    seen = set()
    for p in filepaths:
        log.info("Processing %s ...", p)
        with p.open() as fp:
            for line in fp:
                # Sometimes the same log message ends up in more than one log
                # file, so weed out duplicates:
                if line in seen:
                    continue
                seen.add(line)
                m = re.match(
                    r"(?P<timestamp>\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+[-+]\d\d:\d\d)"
                    r" (?P<target>\S+): ",
                    line,
                )
                if not m:
                    log.warning("Failed to parse Heroku log line: %s", line)
                    continue
                timestamp = datetime.fromisoformat(m["timestamp"])
                target = m["target"]
                message = line[m.end() :].strip()
                yield HerokuLog(
                    timestamp=timestamp, target=target, message=message, line=line
                )


def process_logs(logfiles: Iterable[Path]) -> Events:
    events = Events()
    misses_in_progress = {}
    last_accesses = {}
    for lg in iterlogs(logfiles):
        if lg.target == "app[web.1]":
            entry = json.loads(lg.message)
            timestamp = datetime.fromisoformat(entry["timestamp"])
            fields = entry.get("fields", {})
            if (
                "cache_event" in fields
                and fields["cache_event"] not in ("dump", "dump-error")
                and fields.get("cache") == "zarr-manifests"
            ):
                span_id: str | None = entry.get("span", {}).get("id")
                manifest_path: str = fields["manifest"]
                match fields["cache_event"]:
                    case "miss_pre":
                        assert span_id is not None
                        misses_in_progress[
                            MissKey(span_id=span_id, manifest_path=manifest_path)
                        ] = MissMoment(
                            timestamp=timestamp,
                            approx_cache_len=fields["approx_cache_len"],
                        )
                    case "miss_post":
                        assert span_id is not None
                        key = MissKey(span_id=span_id, manifest_path=manifest_path)
                        if (start := misses_in_progress.pop(key, None)) is not None:
                            end = MissMoment(
                                timestamp=timestamp,
                                approx_cache_len=fields["approx_cache_len"],
                            )
                            events.misses.append(
                                Miss(
                                    manifest_path=manifest_path,
                                    start=start,
                                    end=end,
                                    parallel=len(misses_in_progress),
                                )
                            )
                        last_accesses[manifest_path] = timestamp
                    case "hit":
                        last_access = last_accesses.get(manifest_path)
                        if last_access is not None:
                            recency_index = sum(
                                1 for ts in last_accesses.values() if ts > last_access
                            )
                        else:
                            recency_index = len(last_accesses)
                        events.hits.append(
                            Hit(
                                manifest_path=manifest_path,
                                timestamp=timestamp,
                                last_access=last_access,
                                approx_cache_len=fields["approx_cache_len"],
                                recency_index=recency_index,
                            )
                        )
                        last_accesses[manifest_path] = timestamp
                    case "evict":
                        events.evictions.append(
                            Eviction(
                                manifest_path=manifest_path,
                                timestamp=timestamp,
                                cause=RemovalCause(fields["cause"]),
                            )
                        )
                        last_accesses.pop(manifest_path, None)
                    case other:
                        log.warning(
                            "Invalid 'cache_event' field value %r: %s", other, lg.line
                        )
        elif lg.target == "heroku[web.1]" and lg.message in (
            "Stopping all processes with SIGTERM",
            "Stopping process with SIGKILL",
        ):
            events.killed_misses.extend(
                KilledMiss(
                    manifest_path=k.manifest_path, start=v, killed_at=lg.timestamp
                )
                for k, v in misses_in_progress.items()
            )
            misses_in_progress = {}
            last_accesses = {}
    return events


def summarize(events: Events) -> None:
    miss_duration_stats = Statistics.for_values(
        (m.end.timestamp - m.start.timestamp).total_seconds() for m in events.misses
    )
    print("Miss durations:")
    print(textwrap.indent(str(miss_duration_stats), " " * 4))
    print()

    miss_parallel_stats = Statistics.for_values(m.parallel for m in events.misses)
    print("Miss parallels:")
    print(textwrap.indent(str(miss_parallel_stats), " " * 4))
    print()

    hit_recencies = Counter(hit.recency_index for hit in events.hits)
    print("Hit recencies:")
    print("    Miss:", len(events.misses))
    for recency, qty in sorted(hit_recencies.items()):
        print(f"    {recency}: {qty}")
    print()

    evict_stats = Counter(ev.cause for ev in events.evictions)
    print("Eviction causes:")
    for cause, qty in sorted(evict_stats.items(), key=lambda p: p[0].name):
        print(f"    {cause}: {qty}")


if __name__ == "__main__":
    main()
