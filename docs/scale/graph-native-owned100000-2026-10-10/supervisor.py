#!/usr/bin/env python3
"""Persist the real child exit before the service can unload."""
import datetime
import json
import os
from pathlib import Path
import subprocess
import sys

here = Path(__file__).resolve().parent
def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
state = dict(started_utc=now(), terminal=False, go_exit=None,
             configuration={key: os.getenv(key) for key in
                            ("WF_GRAPH_NATIVE_OWNED_GRANTS", "WF_GRAPH_NATIVE_COMPACTION_LIFETIME")},
             command=["/usr/local/bin/go", "test", "-race", "./internal/graphpublication",
                      "-run", "^TestNativeGraphOwnedGrantRenewalCostAndRecovery/R1$",
                      "-timeout=100m", "-count=1", "-v"])
def save():
    temporary = here / "process-state.json.tmp"
    temporary.write_text(json.dumps(state, indent=2) + "\n")
    temporary.replace(here / "process-state.json")
save()
with (here / "native-race.log").open("w") as output:
    process = subprocess.Popen(state["command"], stdout=output, stderr=subprocess.STDOUT)
    state["go_pid"] = process.pid
    save()
    result = process.wait()
state.update(terminal=True, go_exit=result, ended_utc=now())
save()
sys.exit(result if result >= 0 else 128 - result)
