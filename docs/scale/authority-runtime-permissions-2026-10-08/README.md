# Native authority runtime permissions — 2026-10-08

`AuthoritySubjectAccess` returns an allowlist for the trusted native authority adapter: publish the root/blob subjects and the named stream's INFO/MSG.GET API subjects; subscribe to reply inboxes. Install these as NATS publish/subscribe allow lists. Other subjects must remain denied. The API prefix is explicit for namespace configuration; native domain-routing qualification is separate. Example for `BLOB_AUTH`, `wf.blob.authority`, `$JS.API`:

```conf
permissions: {
  publish: { allow: ["wf.blob.authority.root.*", "wf.blob.authority.blob.*", "$JS.API.STREAM.INFO.BLOB_AUTH", "$JS.API.STREAM.MSG.GET.BLOB_AUTH"] }
  subscribe: { allow: ["_INBOX.>"] }
}
```

Use separate provisioning credentials to create/manage the permanent stream. Runtime identities must not have an additional broad publish grant or account import that restores forbidden APIs. Direct authority publish permission is for trusted protocol adapters, which can still send arbitrary bytes; this is not validation of an untrusted publisher. Administrators must retain the stream/high-water state and not delete/recreate it or relax retention. A server administrator can change user permissions; this policy does not constrain that administrator.

Native R1/R3 controls use actual named principals, with normal authority opening/read-witness/root/blob CAS/census/retirement. Ten exact asynchronous NATS permission errors deny stream update/delete/create/purge/message deletion, unrelated stream metadata access and unrelated publications. Each request leaves stream configuration/count/last-sequence unchanged. Separate provisioning credentials create/delete a control stream. Retirement retains head2 and rejects stale initial publication. Restricted credentials reopen the same store after node0 shutdown/restart, preserve head2 and still receive an exact delete permission denial. The fixture now supports named users and retains authentication on restart. Default fixtures retain their prior connection options.

Development's invalid first-generation fixture and insufficient R3 metadata-placement admission are retained. The fixture now starts from a valid uploading generation and waits for the full metadata peer set, without changing the caller's30s budget. Final development race controls pass. Retained SDK/source/media qualification remains pending. ObjectStore roles, account imports/domains, runtime reference migration, native concurrency/scale/server power-loss, full matrices/24h/million physical drain/default adoption and production online GC remain open.
