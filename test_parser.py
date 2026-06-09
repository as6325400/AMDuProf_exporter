#!/usr/bin/env python3
"""Self-test for MemoryReportParser against a REAL captured AMDuProfPcm sample.

Run: python3 test_parser.py   (exits non-zero on failure; used by CI)

samples/sample_memory_output.txt is real output from
`AMDuProfPcm -m memory --msr --verbose -a -t 1000` on an AMD EPYC 9374F
(uProf 5.3.518). The last data row in that sample is:
    8.50, 5.52, 1.44, 1.11, 0.44, 6.63, 1.87
mapped to the 7 DF-metric columns; latest-sample-wins, so those are the values
we expect after parsing the whole stream.
"""
import os
import sys

from amduprof_exporter import MemoryReportParser

HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLE = os.path.join(HERE, "samples", "sample_memory_output.txt")

EXPECTED = {
    "total_mem_bw": (8.50, "total"),
    "local_dram_read_data_bytes": (5.52, None),
    "local_dram_write_data_bytes": (1.44, None),
    "remote_dram_read_data_bytes": (1.11, None),
    "remote_dram_write_data_bytes": (0.44, None),
    "total_mem_rdbw": (6.63, "read"),
    "total_mem_wrbw": (1.87, "write"),
}


def main() -> int:
    with open(SAMPLE) as fh:
        text = fh.read()
    p = MemoryReportParser()
    p.parse_text(text)
    snap = p.snapshot()

    failures = []

    def check(cond, msg):
        if not cond:
            failures.append(msg)

    check(p.rows_parsed == 10, f"rows_parsed={p.rows_parsed} (want 10)")
    check(set(snap.keys()) == set(EXPECTED), f"metrics={sorted(snap)}")

    for name, (val, kind) in EXPECTED.items():
        if name not in snap:
            failures.append(f"missing metric {name}")
            continue
        info = snap[name]
        check(info["unit"] == "gbps", f"{name} unit={info['unit']} (want gbps)")
        check(info["kind"] == kind, f"{name} kind={info['kind']} (want {kind})")
        check(set(info["values"]) == {"system"}, f"{name} scopes={set(info['values'])}")
        got = info["values"].get("system")
        check(got == val, f"{name} system={got} (want {val})")

    # Metadata/topology above the metrics section must NOT leak in as metrics.
    for bad in ("socket", "ccx", "core_s", "number_of_cores", "iodie"):
        check(bad not in snap, f"metadata leaked as metric: {bad}")

    if failures:
        print("FAIL:")
        for f in failures:
            print("  -", f)
        return 1
    print(f"OK: parsed {p.rows_parsed} rows, {len(snap)} metrics, all assertions passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
