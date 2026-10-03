#!/usr/bin/env python3
"""Plan bounded sustained fault jobs; omitted/failed jobs are not clean seeds."""
import json
import os

ROWS = (
    "journal", "consumer", "cluster", "partition", "worker", "pause",
    "isolation", "workerclock", "serverclockplus", "serverclockminus",
    "fanoutrestart", "blockdisk", "upgrade",
)


def campaign(row, count, duration, start_seed=1):
    if row not in (*ROWS, "all") or type(count) is not int or count not in (1, 20, 200) or duration not in ("10m", "35s"):
        raise ValueError("unsupported row, seed count or duration")
    if type(start_seed) is not int or start_seed < 1 or start_seed + count - 1 >= 2**63:
        raise ValueError("seed range must be positive signed64 integers")
    last_seed = start_seed + count - 1
    # A Cartesian 13 × 200 matrix exceeds GitHub's 256-job limit. Twelve
    # consecutive seeds per job bounds 20-minute Go attempts to four hours,
    # leaving setup/artifact time within the six-hour hosted-runner limit.
    width = 12 if row == "all" else 1
    jobs = [
        {
            "row": selected, "first": first, "last": min(first + width - 1, last_seed),
            "artifact_seed": str(first) if first == min(first + width - 1, last_seed) else f"{first}-{min(first + width - 1, last_seed)}",
        }
        for selected in (ROWS if row == "all" else (row,))
        for first in range(start_seed, last_seed + 1, width)
    ]
    if len(jobs) > 256:
        raise ValueError("campaign exceeds hosted matrix limit")
    return jobs


if __name__ == "__main__":
    jobs = campaign(os.environ["MATRIX_ROW"], int(os.environ["SEED_COUNT"]), os.environ["MATRIX_DURATION"], int(os.environ.get("START_SEED", "1")))
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write("jobs=" + json.dumps(jobs, separators=(",", ":")) + "\n")
