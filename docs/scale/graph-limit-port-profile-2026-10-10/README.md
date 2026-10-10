# Bounded native graph port diagnostic

Optional `WF_GRAPH_LIMIT_PORT_PROFILE=1` decorates the native graph port in the
existing continuation-cap fixture. The regular native opener still checks
configuration, and all graph settings and production cap defaults are retained.
Completed ReadRoot/CASRoot/ReadBlob/CASBlob/Put/Get calls are counted and timed
around each SDK padding stage, including concurrent polling. Durations can
overlap; calls spanning a window are attributed when they complete. They do
not isolate broker RPCs or establish a server-side cause.

The frozen race driver runs only R1/archive with a private 64-entry budget,
24 real SetState operations, two checkpoints and the reserved terminal slot.
Its reviewer checks source, binary, command, actual terminal supervisor, native
fixture assertions and both profiling windows. This is a bounded diagnostic,
not production-cap or latency qualification. The original live 100000-entry
run and its 300-minute watchdog remain unchanged.

## Closed diagnostic

Frozen `431d2d1` passes [independent review](review.json):3398 exact Git
inputs, loaded matching supervisor exit0, native race case46.516s,64 ordered
entries, terminal slot63, two checkpoints and zero forbidden effects.
Each 12-operation window has750 completed port calls (62.5 per SetState):
144 ReadRoot,48 CASRoot,210 ReadBlob,140 CASBlob,70 Put and138 Get.
Padding wall times are4.921s and5.356s, with zero returned port errors.
ReadRoot accounts for1.195s/1.506s of summed call time; no single object
operation dominates either window. The shared VM and race binary affect
latency; these bounded counts cannot establish larger-history scaling or
a broker defect. The actual100000 gate remains open.
