#!/usr/bin/env python3
"""Plan bounded sustained fault jobs; omitted/failed jobs are not clean seeds."""
import json
import os

ROWS = (
    "journal", "consumer", "cluster", "partition", "worker", "pause",
    "isolation", "workerclock", "serverclockplus", "serverclockminus",
    "fanoutrestart", "blockdisk", "upgrade",
)


def campaign(row, count, duration):
    if row not in (*ROWS, "all") or count not in (1, 20, 200) or duration not in ("10m", "35s"):
        raise ValueError("unsupported row, seed count or duration")
    # A Cartesian 13 × 200 matrix exceeds GitHub's 256-job limit. Twelve
    # consecutive seeds per job bounds 20-minute Go attempts to four hours,
    # leaving setup/artifact time within the six-hour hosted-runner limit.
    width = 12 if row == "all" else 1
    jobs = [
        {
            "row": selected, "first": first, "last": min(first + width - 1, count),
            "artifact_seed": str(first) if first == min(first + width - 1, count) else f"{first}-{min(first + width - 1, count)}",
        }
        for selected in (ROWS if row == "all" else (row,))
        for first in range(1, count + 1, width)
    ]
    if len(jobs) > 256:
        raise ValueError("campaign exceeds hosted matrix limit")
    return jobs


if __name__ == "__main__":
    jobs = campaign(os.environ["MATRIX_ROW"], int(os.environ["SEED_COUNT"]), os.environ["MATRIX_DURATION"])
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write("jobs=" + json.dumps(jobs, separators=(",", ":")) + "\n")
