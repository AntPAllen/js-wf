# Packaged SQL schema-startup SIGINT plus original full50000 recovery prepared

New explicit profile `standalone-leaf-startup-sigint` selects the full original50000 recovery test, with SIGINT delivered to the actual packaged child while PostgreSQL's relation lock blocks CREATE INDEX. The blocker remains held through actual exit0/join within10s, zero SQL sessions/locks/rows and absent projection durables. Only then is it released and the complete original fault/rebuild/purge case runs. Original20m scenario/22m SDK/count1/nonrace/twoCPU2GiB remain.

Existing profiles still select SIGTERM; their proof/log fields are preserved. The new profile records explicit SIGINT marker/timestamp/native-log suffix. Independent review requires the selected signal and rejects opposite or missing signal/timestamp; exact boundary log text is checked. Inherited complete50000, child leaf wire, SQL and row rebuild assertions remain mandatory. Manual CI has a sixth explicit profile and carries selected-signal proof mutations; no hosted run is dispatched.

Compilation and Python syntax pass. No native or current full-suite acceptance is claimed. Next: run this original full50000 profile in a retained fresh owned-SQL service, then independent original-source/binary/media/archive review and actual-positive proof mutations. Original24h and full123 race campaigns remain active in isolated sources.
