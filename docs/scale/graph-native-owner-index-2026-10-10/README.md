# Native complete owner-scope index — 2026-10-10

New explicitly provisioned graph namespaces can now use
`OwnerIndexedAuthorityStreamConfig`, `OpenOwnerIndexedNativeAuthority`, and
`OpenOwnerIndexedNativePort`. The wrapper implements `OwnerScopePort`; legacy
NativePort does not. No journal/worker default or existing stream is changed.

The index resides in the same permanent file-backed authority stream, with one
immutable `(owner, scope)` registration per subject. Every blob mutation first
witnesses registration through a conditional quorum-acknowledged publication.
Unknown registration outcomes stop before the blob mutation. Committed unknown
registrations remain discoverable as absent reservations. Unknown blob outcomes
remain registered, including abandoned uploading grants. Index subjects and blob
revisions cannot be deleted/purged under the admitted stream configuration.

The new namespace has a distinct three-pattern subject configuration and a
required mode tag. Legacy adapters reject it. The subject shape also fails the
exact configuration check in pre-index binaries that know nothing about the
tag. Indexed openers reject legacy configuration. This is **fresh provisioning**,
not migration: do not add the mode tag or change the subject shape on an existing
stream. Administrators must preserve configuration and permanent authority
records, as they already must preserve logical heads and generations. No
automatic import or schema upgrade is provided.

Discovery uses the SDK's paginated subject census filtered to the owner hash,
then witnesses the exact canonical bytes of each registration. The SDK consumes
the filtered response total; stream `NumSubjects` describes the whole stream
and cannot count this subset. Errors return no partial list. No concurrent
staging under the same token is allowed. Renewed scope records and final target
content retain their independent authority checks. Full blob/root censuses
recognize index subjects without treating them as graph objects or roots.

## Evidence

The restored race selection passes 21.711 seconds: four namespace admission
cases, native R1/R3 indexed recovery, eleven owner-discovery model cases,
seventeen prior renewal safety cases, native R1/R3 legacy persistence/schema
cases and eight existing mutation/read-witness cases. The common 853-pin corpus
is retained separately. Omitting registration before mutation fails the native
R1 and R3 recovery controls. Exact bypass/source bytes and terminal logs remain
available; source restoration precedes the accepted race selection.

Each native case builds a four-record graph and real prefix compaction on actual
object/authority ports. Sixteen unrelated uploading scopes are closed as
permanent high-water records. Additional cases create an abandoned upload,
drop an index publication, lose an index acknowledgment after commitment, and
lose a blob acknowledgment after commitment. Unknown writes make one attempt.
Fresh adapters after all peers restart discover nine owned registrations,
including two uploading orphans and one absent reservation. Renewal reads nine
scope records without calling full BlobKeys, extends orphan expiries, creates
no reservation grant and publishes no source head. A lost census witness returns
no partial result. Ordinary final compaction verification then commits two live
and two archived records under the original head. Full blob/root discovery
remains valid. The new fixture retains its original two-minute parent watchdog.

This is a same-store in-process peer restart, not OS/VM/power-loss acceptance.
Nine owned scopes and sixteen unrelated scopes do not qualify 100,000 entries
or filtered census pagination at large owned cardinality. The earlier incomplete
test version and a development compile error are preserved and excluded from
accepted evidence. The compile error was a test assertion using a journal field
on a graph root; the corrected assertion checks live/archive graph counts.

## Next work

Expose explicit native journal/worker configuration for this separately
provisioned mode, retain descriptor/reader scope identity, and verify recovery
through actual workers. Qualify index census/registration cost for a large owned
grant set, including pagination and uncertainty, before rerunning actual
100,000-entry padding. Existing namespaces require a separate proven complete
migration; they remain on full discovery. Admission and collection remain off;
original full scale, retention, fault, majority, soak and rollout gates stay open.

```sh
go test -race ./internal/graphpublication -run '^(TestOwnerIndexedNativeNamespaceAdmission|TestNativeGraphOwnerScopeIndexRecovery|TestGraphCompactionOwnerScopeDiscovery|TestGraphCompactionIntentRenewalScopesAndFences|TestNativeGraphAuthorityPersistenceAndSchemaIsolation|TestNativeGraphMutationAcrossReadWitnesses)$' -count=1 -v
go test ./sim -run '^TestPinnedRegressionCorpus$' -count=1 -v
python3 docs/scale/graph-native-owner-index-2026-10-10/review.py
```
