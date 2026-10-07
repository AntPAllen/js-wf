# Reviewer development failures

These offline reviewer failures came from incorrect count and JSON field-name assumptions. The retained graph test has five replayed records, and Snapshot serializes its Runtime pointer as `runtime_checkpoint`. Corrected review passed against the unchanged original source, native logs, stores and executable. No native test was rerun to obtain acceptance.
