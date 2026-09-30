#!/usr/bin/env python3
"""Account for recorded append-renew time; no inference about server causality."""
import json
from pathlib import Path
root = Path(__file__).resolve().parent
rows = json.loads((root / "mixed-four-faults-65-operations.json").read_text())
result = {}
for invocation in ("mixed-00-0", "mixed-05-0"):
    renewals = [r for r in rows if r["Type"] == "mixedsignal" and r["ID"] == invocation and r["Operation"] == "lease_renew_append"]
    result[invocation] = {
        "renewals": len(renewals),
        "errors": sum(bool(r["Error"]) for r in renewals),
        "gate_seconds": sum(r["LeaseGateWait"] for r in renewals) / 1e9,
        "update_seconds": sum(r["LeaseUpdateDuration"] for r in renewals) / 1e9,
        "longest_updates": [{"end": r["At"], "index": r["JournalIndex"], "seconds": r["LeaseUpdateDuration"] / 1e9} for r in sorted(renewals, key=lambda r: r["LeaseUpdateDuration"], reverse=True)[:4]],
    }
print(json.dumps({"events": len(rows), "append_renewals": result}, indent=2))
