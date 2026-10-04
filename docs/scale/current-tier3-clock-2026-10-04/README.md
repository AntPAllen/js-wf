# Executed-source clock shard qualification

Complete terminal successful worker-clock shards from run 37164231641 at exact
`79915ca41a5c5a23b9997eea5f3f66d82530ee30` pass independent review:

| Seeds | Job | Invocations | Entries | Faults | Independent history operations |
| --- | --- | ---: | ---: | ---: | ---: |
| [27–39](worker-clock-27-39/) | 111324439636 | 39,452 | 437,316 | 247 | 50,724 |
| [40–52](worker-clock-40-52/) | 111324439602 | 37,632 | 416,582 | 247 | 48,384 |
| [66–78](worker-clock-66-78/) | 111324439609 | 39,984 | 443,511 | 247 | 51,408 |
| [79–91](worker-clock-79-91/) | 111324439586 | 40,012 | 443,742 | 247 | 51,444 |
| Total | | 157,080 | 1,741,151 | 988 | 201,960 |

Every seed executes ten minutes and passes raw/source/clock verification plus
all three source-identical production history models. Worst terminal/progress
type p99 is 5.254787923/0.80936262 s. All four compact proofs retain actual workload/
model executables and complete raw/model evidence.

Physical-archive scope differs: 27–39's complete canonical store archive is
hashed and retained in RAM/GitHub, not Git or reopened; 40–52, 66–78 and 79–91 store artifacts are
referenced only. Their executable hashes match the former's verified
actual bytes; associated SDK original/overlay files are explicitly reconstructed
from that provider. Consult each shard's manifest and provenance limitations.

Failed full parent, remaining 200-seed row, complete matrix and actual 24-hour
full-matrix soak remain open. No failed case is repaired, server cause attributed
or source revision promoted by these four individual shard qualifications.
