# Validate full authority before metadata decisions — 2026-10-09

## Defect and fix

Before this guard, metadata-only Inspect/Begin accepted invalid publication tokens, malformed archive structures and malformed retained-reader snapshots. Reader/payload paths then rejected some descriptors later as unknown outcomes. The retained baseline log reproduces 21 failing assertions, including actual metadata successes and later rejection. It remains preserved.

Journal root observation now uses the public protocol snapshot codec to validate the entire authority image before interpreting its cursor. This checks the live/named forests, token, root limits and reader snapshots, then the existing cursor/lifecycle checks apply. No payload object is read and no ownership grant is created by this structural validation. All cursor versions share the guard; v6 logical offsets retain their existing checks.

## Controls and executed scope

Seventeen mutations cover missing/invalid token, offset beyond count, offset on either side of archive/request, missing pointer, wrong cursor schema, missing/mismatched archive/live population, malformed archive frontier/hash, duplicate/unknown stream and duplicate/invalid-expiry/malformed-snapshot reader. Each runs Inspect, Begin, Open, Append and Compact: 85 negative operations must return `ErrGap` with zero reader CAS, grant reads or payload reads. Valid metadata succeeds; valid reader acquisition reaches an explicit CAS sentinel, proving the probe does not reject everything.

The fixture captures an actual modeled first-compaction authority image, independently clones it per case and mutates only one field. JSON replacements preserve field order. Structural invalidity is distinguished from later ownership failure. The underlying scenario also verifies ordinary two-compaction/read/collection/retirement behavior.

All 31 journal `TestGraph` groups pass normal **19.712s**. Five focused descriptor/checkpoint-index/archive groups pass race **45.899s**, including all 85 negatives. All 762 saved regression traces replay normal **6.554s** with this guard. Earlier test fixture syntax and actual metadata-admission failures remain preserved. No selected group skips. Source hashes and executed review are included. This is development component evidence, not independent frozen-source/full release qualification.

Commands: `go test ./journal -run '^TestGraph' -count=1 -v`; `go test -race ./journal -run '^(TestGraphArchiveDescriptor.*|TestGraphCheckpointIndex.*|TestGraphCheckpointArchive.*)$' -count=1 -v`; `go test ./sim -run '^TestPinnedRegressionCorpus$' -count=1 -v`.

## Limits

This validates authority structure and cursor consistency, not physical payload presence, owned frame/entry contents or authenticity of forged but internally well-shaped roots. Native adapter fault/corruption admission, arbitrary concurrent mutations, current complete/extended 149-family qualification, import/deployment compatibility, worker/lease/Signal/Start/lifecycle cuts, physical process/storage/power loss, scale and all original broader gates remain open. Public continuation admission and production collection remain disabled.

The separate 1,000-seed compaction family race completed at compiled d48fd94 with all 14 modes and exact replay; it excludes this later guard. Its result cannot qualify the complete current source.
