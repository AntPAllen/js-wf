# Ordered state admission observations

Native source `271bbd2e5b90bc78c33e8c80d9432ea0a3402d1e`, normal SDK2906133, hash
`3a6db304180219100a4af24d27c853e10f3c97eca79fb641d5d0fa839a61ccff`, diagnostic PASS23.78s. Actual2.15 five-server copies
were newly verified against3,887 donor files, which remained unchanged. Source
Go/module inputs, actual SDK/server executable identities and closure verified;
external compiler inputs are not exhaustive.

Standalone state collection accepted88,068 complete values in1.896566660s, with a
20s watch deadline, creation1.2694s and explicit initial-completion barrier. The
subsequent original concurrent87920 audit accepted87920 invocations/journals/
terminals and969925 entries in8.838345502s. Its state watch had a2s deadline,
creation0.1235s and initial completion1.1675s. Both state source cuts were unchanged
and consumers returned to0. Public leader-routed consumer metadata confirms R1
memory/AckNone watch configuration; this is not a local physical-store witness.

This ordered comparison warms the fixture. It does not reproduce or establish the
cause of the earlier failed admission, prove isolated speed ratios, or qualify bulk
latency/fault/24h gates. It confirms the generic per-call wrapper gives the complete
initial-state stream only2s even when the caller audit has20s available.

Complete file-byte preservation uses the accepted cached point proof as a pinned
base. All 4,451 logical files are verified: 2,359
unchanged files/1,813,932,703 bytes reference that base, remaining data in
80,326,749-byte delta `9305a1e35fa1b639116b4efb9087a20dd40d066c62599ed0df1d27f3dd47a02f`. Every base/delta member,
part and complete virtual tree was independently read back. Reconstruct only to
fresh paths; original donor and closed disposable copies remain at preservation.
