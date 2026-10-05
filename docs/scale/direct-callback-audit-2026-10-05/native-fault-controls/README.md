# Direct callback native consumer fault race proof

Executed4f47f0b73a77ce2b4cb5e9eca4bac897b343aa51, actual race SDK SHA256
`f120d8db3f11b5718f02521d8c1e53f29b1ee709d87a06d4b4e10707daedad6f`. R3 v2.15.0 servers linked into observed SDK.
Native PASS37.05s; all selected source/executable/race/VCS/module/closure checks
reviewed independently. No separate external server executable claim.

Consumer-leader library shutdown occurs with4039 pending records on actualR3
memory/AckNone audit cursor. Complete report1500 INV/1500 journals/6000 entries/
1500 terminal succeeds in1.946857856s, one reconnect. Cancellation at128 journal
visits returns context.Canceled, elapsed353.603168ms. Both audit streams have
zero consumers after cleanup (asserted in the executed test). The direct callback
candidate uses the same shared bound/gap/order/replay/two-resume invariant body.
These are race correctness controls; elapsed times do not prove performance.

Full1427-member archive read back, one24891585-byte part, SHA256
`6693b32cbe9588cb851f89f1129481276ca0761f10baebdcbd1c6fb1b6b29014`.
Selected680 Git Go/module inputs retained; external compiler/toolchain
inputs are not exhaustive. No OS SIGKILL, R1 failover, legacy, full400k capacity,
public-default or24h adoption/qualification. Legacy and fullcapacity are next.
