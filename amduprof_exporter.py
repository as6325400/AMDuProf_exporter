#!/usr/bin/env python3
"""AMDuProfPcm memory-bandwidth Prometheus exporter.

Wraps AMD uProf's ``AMDuProfPcm`` command-line tool, which streams a fresh
CSV report to stdout every sampling interval, and exposes the parsed metrics
on a Prometheus ``/metrics`` endpoint so they can be scraped into Grafana.

Target command (company requirement):

    /opt/AMDuProf_5.3-518/bin/AMDuProfPcm -m memory --msr --verbose -a -t 1000

Unlike NVML (query-per-scrape), AMDuProfPcm is a *streaming* tool: we launch it
once as a long-lived child process, a reader thread keeps the "latest snapshot"
up to date as new sample rows arrive, and Prometheus scrapes just read that
snapshot. If the child dies it is restarted with backoff.

Real output format (AMD EPYC 9374F, uProf 5.3.518, ``-m memory -a``):

    ... metadata / topology (comma-separated) ...
    Profile Time: 2026/06/02 10:26:42:615
    DF METRICS,,,,,,,
    System (Aggregated),,,,,,,
    Total Mem Bw (GB/s),Local DRAM Read Data Bytes(GB/s),...,Total Mem WrBw (GB/s),
    Profiling started.
    12.29,7.03,2.81,1.40,1.05,8.43,3.86,
    11.46,4.81,4.22,1.54,0.88,6.35,5.11,
    ...

i.e. CSV where *columns are metrics* and *each row is one time sample*. A
"scope" line (``System (Aggregated)``, or ``Package 0`` with ``-A package``)
sets which aggregation level the following rows belong to. The parser is in
``MemoryReportParser``; use ``--dump-raw`` to capture output and ``--parse-file``
to verify what it extracts offline.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
from typing import Iterable, Optional

from prometheus_client import REGISTRY, start_http_server
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

log = logging.getLogger("amduprof_exporter")

DEFAULT_BIN = "/opt/AMDuProf_5.3-518/bin/AMDuProfPcm"
DEFAULT_PORT = 9836  # nvml_exporter uses 9835; keep AMD next door.
METRIC_PREFIX = "amd_uprof"
SCOPE_LABEL = "scope"

# Only these (substring) keywords are exported by default, to skip any
# incidental non-memory tables in --verbose output. Use --all-metrics to lift.
DEFAULT_KEYWORDS = ("mem", "bw", "bandwidth", "dram", "df", "umc")

# A numeric value cell.
_NUM_RE = re.compile(r"^[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?$")
_NA_TOKENS = {"n/a", "na", "-", "--", "nan", "null", ""}

# Unit suffix appended to a sanitized metric name, keyed by the unit found in
# parentheses, e.g. "Total Mem Bw (GB/s)" / "Local DRAM Read Data Bytes(GB/s)".
_UNIT_SUFFIX = {
    "gb/s": "gbps", "gib/s": "gibps", "mb/s": "mbps", "mib/s": "mibps",
    "gbps": "gbps", "ghz": "ghz", "mhz": "mhz", "%": "pct", "pct": "pct",
}
_UNIT_IN_NAME_RE = re.compile(r"\(([^)]*)\)\s*$")

# Aggregation-scope line, e.g. "System (Aggregated)", "Package 0", "Socket-1".
_SCOPE_RE = re.compile(
    r"^(package|socket|core|ccx|ccd|node|numa|thread|die|umc|channel)\s*[-_ ]?(\d+)\b",
    re.IGNORECASE,
)


def _is_num(tok: str) -> bool:
    return bool(_NUM_RE.match(tok)) or tok.lower() in _NA_TOKENS


def _to_float(tok: str) -> Optional[float]:
    if tok.lower() in _NA_TOKENS:
        return None
    try:
        return float(tok)
    except ValueError:
        return None


def _sanitize(name: str) -> str:
    """'Total Mem Bw (GB/s)' -> 'total_mem_bw' (unit handled separately)."""
    name = _UNIT_IN_NAME_RE.sub("", name)
    name = name.lower()
    return re.sub(r"[^a-z0-9]+", "_", name).strip("_")


def _unit_suffix(label: str) -> str:
    m = _UNIT_IN_NAME_RE.search(label)
    return _UNIT_SUFFIX.get(m.group(1).strip().lower(), "") if m else ""


def _scope_of(text: str) -> Optional[str]:
    """Normalize a scope line to a label value, or None if it isn't one."""
    low = text.strip().lower()
    if "aggregated" in low or low == "system":
        return "system"
    m = _SCOPE_RE.match(low)
    return f"{m.group(1)}{m.group(2)}" if m else None


def _canonical_bw(sanitized: str) -> Optional[str]:
    """Return 'read'|'write'|'total' for the headline mem-bandwidth metrics.

    Matches 'Total Mem Bw/RdBw/WrBw'. Deliberately does NOT match the granular
    'Local/Remote DRAM Read/Write Data Bytes' rows (no 'mem'/'bw' in their
    names), so those stay as their own series instead of colliding here.
    """
    if "mem" not in sanitized and "bw" not in sanitized and "bandwidth" not in sanitized:
        return None
    if re.search(r"(^|_)(rd|read)", sanitized):
        return "read"
    if re.search(r"(^|_)(wr|write)", sanitized):
        return "write"
    if "bandwidth" in sanitized or sanitized.endswith("_bw") or "_bw_" in sanitized:
        return "total"
    return None


class Column:
    __slots__ = ("name", "unit", "kind", "export")

    def __init__(self, raw: str, all_metrics: bool):
        self.name = _sanitize(raw)
        self.unit = _unit_suffix(raw)
        self.kind = _canonical_bw(self.name)
        self.export = bool(self.name) and (
            all_metrics or self.kind is not None
            or any(k in self.name for k in DEFAULT_KEYWORDS)
        )


class MemoryReportParser:
    """Parse streamed AMDuProfPcm CSV into a {metric: {scope: value}} snapshot.

    State machine over comma-separated lines:
      * "<X> METRICS"        -> section marker; arms header expectation
      * "System (Aggregated)"/"Package 0"  -> sets current scope
      * "<metric>,<metric>,..."  (all text) -> column header (only when armed)
      * "<num>,<num>,..."    (all numeric)  -> one sample row -> update snapshot
    Metadata/topology above the first section is ignored because the header is
    only accepted right after a section/scope line.
    """

    def __init__(self, all_metrics: bool = False) -> None:
        self.all_metrics = all_metrics
        # sanitized_name -> {"unit","kind","values": {scope: float}}
        self._metrics: dict[str, dict] = {}
        self._columns: Optional[list[Column]] = None
        self._scope: str = "system"
        self._expect_header: bool = False
        self.rows_parsed = 0
        self.parse_errors = 0

    def snapshot(self) -> dict[str, dict]:
        return self._metrics

    def feed_line(self, line: str) -> bool:
        """Feed one raw line. Returns True if a data (sample) row was parsed."""
        fields = [f.strip() for f in line.rstrip("\n").split(",")]
        while fields and fields[-1] == "":
            fields.pop()
        non_empty = [f for f in fields if f]
        if not non_empty:
            return False

        # Single-field lines: section marker / scope / noise.
        if len(non_empty) == 1:
            val = non_empty[0]
            if "METRIC" in val.upper():
                self._columns = None
                self._expect_header = True
                return False
            sc = _scope_of(val)
            if sc:
                self._scope = sc
                self._expect_header = True
                return False
            return False  # "Profiling started.", "CPU Topology:", banners, etc.

        # Data row: every populated cell is numeric.
        if all(_is_num(f) for f in non_empty):
            return self._feed_data(fields)

        # Header row: every populated cell has a letter (metric names). Only
        # trust it right after a section/scope line, so topology rows like
        # "Socket, CCX, Core(s)" in the preamble are ignored.
        if self._expect_header and all(re.search(r"[A-Za-z]", f) for f in non_empty):
            self._columns = [Column(f, self.all_metrics) for f in fields]
            self._expect_header = False
            for c in self._columns:
                if c.export and c.name not in self._metrics:
                    self._metrics[c.name] = {"unit": c.unit, "kind": c.kind, "values": {}}
            return False

        return False  # mixed metadata like "Number of Cores :,64"

    def _feed_data(self, fields: list[str]) -> bool:
        if not self._columns:
            return False
        any_set = False
        for col, raw in zip(self._columns, fields):
            if not col.export:
                continue
            v = _to_float(raw)
            if v is None:
                continue
            self._metrics[col.name]["values"][self._scope] = v
            any_set = True
        if any_set:
            self.rows_parsed += 1
        return any_set

    def parse_text(self, text: str) -> None:
        for ln in text.splitlines():
            self.feed_line(ln)


class AMDuProfRunner:
    """Owns the AMDuProfPcm child process and keeps the latest parsed snapshot."""

    def __init__(self, argv: list[str], all_metrics: bool = False) -> None:
        self.argv = argv
        self.parser = MemoryReportParser(all_metrics=all_metrics)
        self._lock = threading.Lock()
        self._snapshot: dict[str, dict] = {}
        self._last_update = 0.0
        self._proc: Optional[subprocess.Popen] = None
        self._stop = threading.Event()
        self.alive = False
        self.restarts = 0
        self._thread = threading.Thread(target=self._run_loop, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        p = self._proc
        if p and p.poll() is None:
            try:
                p.terminate()
                p.wait(timeout=3)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass

    def get(self) -> tuple[dict[str, dict], float, bool]:
        with self._lock:
            snap = {k: {**v, "values": dict(v["values"])} for k, v in self._snapshot.items()}
            return snap, self._last_update, self.alive

    def _publish(self) -> None:
        with self._lock:
            src = self.parser.snapshot()
            self._snapshot = {k: {**v, "values": dict(v["values"])} for k, v in src.items()}
            self._last_update = time.monotonic()

    def _run_loop(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                log.info("Launching: %s", " ".join(shlex.quote(a) for a in self.argv))
                self._proc = subprocess.Popen(
                    self.argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    bufsize=1, universal_newlines=True,
                )
            except (FileNotFoundError, PermissionError) as e:
                self.alive = False
                log.error("Cannot launch AMDuProfPcm (%s): %s", self.argv[0], e)
                if self._stop.wait(min(backoff, 30)):
                    return
                backoff = min(backoff * 2, 30)
                continue

            self.alive = True
            backoff = 1.0
            assert self._proc.stdout is not None
            for line in self._proc.stdout:
                if self._stop.is_set():
                    break
                if self.parser.feed_line(line):
                    self._publish()  # publish right after each new sample row
            self._publish()
            self.alive = False
            rc = self._proc.poll()
            if self._stop.is_set():
                return
            self.restarts += 1
            log.warning("AMDuProfPcm exited (rc=%s); restarting in %.0fs", rc, backoff)
            if self._stop.wait(backoff):
                return
            backoff = min(backoff * 2, 30)


class AMDuProfCollector:
    """Prometheus custom collector that emits the runner's latest snapshot."""

    def __init__(self, runner: AMDuProfRunner, staleness: float) -> None:
        self.runner = runner
        self.staleness = staleness

    def collect(self) -> Iterable:
        snap, last_update, alive = self.runner.get()
        now = time.monotonic()
        age = (now - last_update) if last_update else float("inf")
        fresh = last_update > 0 and age <= self.staleness

        up = GaugeMetricFamily(f"{METRIC_PREFIX}_up",
                               "1 if AMDuProfPcm is running and producing fresh data, else 0")
        up.add_metric([], 1 if (alive and fresh) else 0)
        yield up

        proc_alive = GaugeMetricFamily(f"{METRIC_PREFIX}_process_running",
                                       "1 if the AMDuProfPcm child process is currently alive")
        proc_alive.add_metric([], 1 if alive else 0)
        yield proc_alive

        age_g = GaugeMetricFamily(f"{METRIC_PREFIX}_snapshot_age_seconds",
                                  "Seconds since the last parsed sample row")
        age_g.add_metric([], age if last_update else -1.0)
        yield age_g

        restarts = CounterMetricFamily(f"{METRIC_PREFIX}_process_restarts_total",
                                       "Times the AMDuProfPcm child process was restarted")
        restarts.add_metric([], float(self.runner.restarts))
        yield restarts

        parse_errs = CounterMetricFamily(f"{METRIC_PREFIX}_parse_errors_total",
                                         "Number of report lines that failed to parse")
        parse_errs.add_metric([], float(self.runner.parser.parse_errors))
        yield parse_errs

        if not fresh:
            return

        bw = GaugeMetricFamily(
            f"{METRIC_PREFIX}_mem_bandwidth_gbps",
            "Approximate memory bandwidth in GB/s (AMDuProfPcm -m memory)",
            labels=[SCOPE_LABEL, "kind"],
        )
        emitted_canonical = False

        for sanitized, info in sorted(snap.items()):
            values = info["values"]
            if not values:
                continue
            if info["kind"] is not None:
                for scope, val in sorted(values.items()):
                    bw.add_metric([scope, info["kind"]], val)
                emitted_canonical = True

            mname = f"{METRIC_PREFIX}_{sanitized}"
            if info["unit"]:
                mname = f"{mname}_{info['unit']}"
            fam = GaugeMetricFamily(mname, f"AMDuProfPcm metric '{sanitized}'",
                                    labels=[SCOPE_LABEL])
            for scope, val in sorted(values.items()):
                fam.add_metric([scope], val)
            yield fam

        if emitted_canonical:
            yield bw


def build_argv(args: argparse.Namespace) -> list[str]:
    argv = [args.amduprof_bin, "-m", args.metric, "-a", "-t", str(args.interval)]
    if args.msr:
        argv.append("--msr")
    if args.verbose_tool:
        argv.append("--verbose")
    if args.extra_args:
        argv.extend(shlex.split(args.extra_args))
    return argv


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--port", type=int, default=DEFAULT_PORT,
                   help=f"HTTP port (default: {DEFAULT_PORT})")
    p.add_argument("--addr", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    p.add_argument("--amduprof-bin", default=os.environ.get("AMDUPROF_BIN", DEFAULT_BIN),
                   help=f"Path to AMDuProfPcm (default: {DEFAULT_BIN})")
    p.add_argument("--metric", default="memory",
                   help="AMDuProfPcm -m metric group (default: memory)")
    p.add_argument("--interval", type=int, default=1000,
                   help="AMDuProfPcm -t interval in ms (the tool may raise it; default: 1000)")
    p.add_argument("--no-msr", dest="msr", action="store_false", default=True,
                   help="Do not pass --msr (default: pass --msr; needs root)")
    p.add_argument("--no-verbose-tool", dest="verbose_tool", action="store_false",
                   default=True, help="Do not pass --verbose to AMDuProfPcm")
    p.add_argument("--extra-args", default="",
                   help="Extra args appended to the AMDuProfPcm command (quoted)")
    p.add_argument("--all-metrics", action="store_true",
                   help="Export every parsed column, not just memory/DF metrics")
    p.add_argument("--staleness", type=float, default=15.0,
                   help="Seconds after which the snapshot is stale and gauges are "
                        "suppressed (default: 15; the tool often samples every ~4s)")
    p.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"),
                   help="Logging level (default: INFO)")
    p.add_argument("--dump-raw", action="store_true",
                   help="Run AMDuProfPcm and echo its raw output to stdout, then exit "
                        "(use to capture a sample for parser tuning)")
    p.add_argument("--parse-file", metavar="FILE",
                   help="Parse a captured AMDuProfPcm sample file, print the metrics "
                        "that would be exported, then exit")
    return p.parse_args()


def _do_dump_raw(args: argparse.Namespace) -> int:
    argv = build_argv(args)
    log.info("dump-raw: %s", " ".join(shlex.quote(a) for a in argv))
    try:
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, universal_newlines=True)
    except (FileNotFoundError, PermissionError) as e:
        log.error("Cannot launch AMDuProfPcm: %s", e)
        return 1
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
    except KeyboardInterrupt:
        proc.terminate()
    return 0


def _do_parse_file(args: argparse.Namespace) -> int:
    parser = MemoryReportParser(all_metrics=args.all_metrics)
    with open(args.parse_file, "r", errors="replace") as fh:
        parser.parse_text(fh.read())
    snap = parser.snapshot()
    if not snap:
        print("No metrics parsed. The format may differ from what the parser expects.")
        return 2
    print(f"Parsed {parser.rows_parsed} sample rows into {len(snap)} metrics:\n")
    for sanitized, info in sorted(snap.items()):
        mname = f"{METRIC_PREFIX}_{sanitized}" + (f"_{info['unit']}" if info["unit"] else "")
        kind = f" [bw:{info['kind']}]" if info["kind"] else ""
        print(f"  {mname}{{{SCOPE_LABEL}=...}}{kind}")
        for scope, val in sorted(info["values"].items()):
            print(f"      {SCOPE_LABEL}={scope}  ->  {val}")
    return 0


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=args.log_level.upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.parse_file:
        return _do_parse_file(args)
    if args.dump_raw:
        return _do_dump_raw(args)

    if not os.path.exists(args.amduprof_bin):
        log.warning("AMDuProfPcm not found at %s — exporter will keep retrying. "
                    "Set --amduprof-bin or AMDUPROF_BIN.", args.amduprof_bin)

    runner = AMDuProfRunner(build_argv(args), all_metrics=args.all_metrics)
    REGISTRY.register(AMDuProfCollector(runner, staleness=args.staleness))

    stop_evt = threading.Event()

    def _shutdown(signum, _frame):
        log.info("Received signal %d, shutting down", signum)
        runner.stop()
        stop_evt.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    runner.start()
    log.info("Listening on http://%s:%d/metrics", args.addr, args.port)
    start_http_server(args.port, addr=args.addr)
    stop_evt.wait()
    return 0


if __name__ == "__main__":
    sys.exit(main())
