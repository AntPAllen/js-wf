# Retained Tier2 journal smoke: native pass, producer acceptance failure

Executed source `584aac5ceb8b4d2b5f732470ba05a01c674470f0`.
Actual SDK PID453246, executable SHA256
`88e647aa792821f71e665ead25778962ac57d8b77d5938dd7b9480f5686dc1d8`.
Normal profile, seed1, original35s smoke, native PASS46.69s.
One journal-leader fault admitted at30.004s, healed34.383s; seven batches,
196 invocations/journals/terminals and2156 entries. This is not ten-minute
qualification or full-matrix acceptance.

The first producer exited1 after native exit0: test2json without `-t` omitted
Elapsed and the original duration checker rejected it. Original converted events,
acceptance failure and logs remain intact. Independent conversion of the same
closed native log with `go tool test2json -t -p js-wf/integration` passes the
unchanged duration checker. Go's converter parses the test elapsed46.69s from
native output; generated event timestamps/package elapsed describe conversion,
not native execution. The next producer now requests `-t`.

Independent review binds actual SDK identity, exact producer/observer/checker,
668 selected Git files and3287 selected external inputs before/after. Four actual
server observations cover initial three nodes and the restarted journal leader;
all observed SDK/server PIDs are gone. Server executable bytes were captured
through live `/proc`, with PID start ticks checked across copying. These are
point observations, not exhaustive process lifetime coverage. Five invalid
ownership/argument controls reject unrelated stores, duplicate names, mismatched
node/store, missing arguments and foreign server names.

Complete closed originals, executables, selected source and review are retained
in archive parts with member and concatenated SHA256 readback. Native retained
integrity was checked by the fixture; no independent copied-store audit is claimed.
Full thirteen-row/200-seed qualification remains open.
