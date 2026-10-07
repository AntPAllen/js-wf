# Operator daemon connection startup cancellation

At recorded source `5b65aba`, `project` and `tombstone-loop` register SIGTERM/SIGINT before dialing. A context-aware dialer cancels DNS/dialing and closes sockets during connection startup; the socket error becomes cancellation only when that startup cancellation actually closed it. After a successful connection, the startup callback is disarmed and existing API cancellation/normal client shutdown behavior remains.

## Native evidence

- [Controlled INFO/PONG race cases](native-race/): actual SDK2193543, eight distinct compiled CLI children, all eight command/stage/signal combinations exit0 while the server response remains withheld. Signal-to-join1.002–1.007s (including default race-runtime exit delay), under the original new-case3s bound. Actual child argv/executable hashes/birth records and complete handshake bytes are preserved. No API publication occurs. Original count1/3m/twoGoCPU/1GiB profile is unchanged.
- [Existing daemon regression](daemon-regression-race/): actual SDK2203014, original default/domain daemon suite and all eight startup/running signal cases plus two independent stream-not-found fatal controls pass; SDK25.564s. Actual R1/default and R3/WFOPS embedded peers and original client-trace startup gate retain their original scope.
- [Previous CLI comparison](previous-cli-baseline/): exact clean sparse checkout `3d04417`, matching race CLI and eight actual children. Every held INFO/PONG SIGTERM/SIGINT case exits by signal (-15/-2), establishing the missing startup handler. All19008 present tracked Git files are independently compared with Git; inherited sparse historical archive omissions are recorded. No compiled input is omitted.

Both finite test runs have original retained successful user-service identities, exact source-before/source-after inventories, parent/child executable identities and complete archive/member verification. Independent reviewers verify2230 selected Git inputs for each corrected-source native run. Initial reviewer self-argv closure rejection and sparse historical proof-part assertion failure are retained; corrected read-only review does not rerun the native cases or change their artifacts. Complete native archives and S3 receipts accompany each group.

## Limits

INFO/PONG cases use controlled protocol fixtures, not an actual NATS cluster. They do not qualify TLS, authentication, DNS failure, leaf faults, SQL startup, or complete release matrices. Existing real default/domain daemon regression is a separate result. The persistent operator CI adds a dedicated `standalone-connection` row; hosted acceptance is not claimed. Production NATS remains official2.15.0. Safe onlineGC, full native matrices, original million physical drain, full123 normal100k and actual24h remain separate requirements.
