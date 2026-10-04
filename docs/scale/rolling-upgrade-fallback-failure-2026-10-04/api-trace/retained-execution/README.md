# Retain the actual workload executable for future opt-in diagnostics

Future `upgrade_api_trace=true` runs compile once with the same race flag and execute the retained binary through `go tool test2json`, preserving the original test selector, count and20m test timeout. Execution uses the integration package directory, matching `go test ./integration`. Default rows keep their existing command.

`retained-sdk/` stores actual executable/build info, runner/commands/relevant environment, complete Go JSON events/stderr, selected dependency listing, pre/post hashes, captured input bytes and execution metadata. It is included recursively in the original archive; compact metadata/events also upload with raw diagnostics. Nonzero native outcomes propagate through the existing pipefail/tee/render pipeline. Changed selected inputs or executable bytes are rejected; existing artifact roots are refused.

Selected Go/Cgo/test/module inputs are a superset, not exhaustive assembly/embed/generated or hermetic compiler provenance. Dependency test files may be selected without being linked. Missing generated inputs are recorded. Captured local input bytes are checked against exact Git source. Recording a binary does not independently qualify its named test, original state, matrices or24h gate.

Real isolated Git/Go controls verify passing and intentionally failing actual compiled tests, identical emitted/retained JSON, actual executable SHA, all captured input bytes, refusal to overwrite prior roots and rejection when an executed test changes an imported compiled source file. Controls pass in3.879s. Whole-repository driver validation remains pending after commit. No production/simulation/Tier1 producer code, deadline or retry policy changes.

The already launched seed15 diagnostic37194088941 executes520316e and predates this retention change. It cannot retroactively acquire its missing workload executable or build-input capture.
