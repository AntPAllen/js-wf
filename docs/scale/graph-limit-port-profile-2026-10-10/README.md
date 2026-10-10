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
