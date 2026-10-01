# Worker process event evidence

Production `wf-worker` now supports optional `-events-file FILE`. Fencing and
start/signal/suspended/timer/fallback repair callbacks retain actual typed events,
including uncertain publication outcomes. The JSONL envelope has version1,
worker ID, actual PID, UTC process-session start, session sequence and kind.
Use one file per process. A reopened complete file appends a new session whose
sequence starts at1. An incomplete final line is refused without truncation.

The queue holds256 records. Observer callbacks do not wait for disk I/O. A full
queue or write/sync/close error causes the runner to stop with an error. Graceful
exit drains and syncs; SIGKILL can lose queued or unsynced records. This proof
covers graceful process evidence, not full mixed chaos or hard-kill completeness.

## Evidence

- `focused-race.log`: logger and actual worker subprocess tests PASS3.568s.
  Production runner loads a real plugin against a real R1 server, repairs an
  invocation lacking a run wakeup, returns42 and handles SIGTERM successfully.
- `worker-events.jsonl` and `result.txt`: child PID71787, generation1, two actual
  acknowledged missing-journal start repairs, continuous session sequences.
- `runner-regression.log`: static, KV, automatic assignment, fallback timers and
  continuation plugin checks PASS5.665s under race instrumentation.
- `vet.log`: successful vet, with no output.
- `excluded-fixture-failure.log`: initial child correctly rejected auto mode on
  an existing native stream. The fixture now explicitly selects native mode for
  its verified single server and detects early child exit. This is not acceptance.

Focused unit controls exercise append sessions, a blocked writer with nonblocking
callbacks, queue overflow, incomplete append tails and write/sync/close errors.
Existing compiled cache binaries were removed to recover disk space; no campaign
store or source evidence was deleted.
