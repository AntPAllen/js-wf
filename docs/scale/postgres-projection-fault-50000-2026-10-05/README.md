# Full PostgreSQL projection recovery evidence

| Executed source | Scope | Verdict |
| --- | --- | --- |
| f87c422 | Original combined full 50,000 case | [Failed; complete originals](failed/) |
| e6c124f | Refreshed clients/workers, source readiness and tracing | [Failed at ordered WF_INV Fetch](refreshed/) |
| 9fbfa16 | Continuous ordered reader, full 50,000 and original 20m | [Qualified recovery and SQL rebuild equality](streaming/) |
| 9fbfa16 production /11997cd helper | Full audit of fresh closed-store copies | [Qualified complete integrity and queue drain](copied-retained-audit/) |

Historical failures remain failed and their server-side causes remain unconfirmed.
The qualified native case does not cover PostgreSQL server SIGKILL, additional
combined fault profiles, full matrices or 24 hours. Original NATS stores were
never reopened; only byte-verified copies were opened for the independent audit.
