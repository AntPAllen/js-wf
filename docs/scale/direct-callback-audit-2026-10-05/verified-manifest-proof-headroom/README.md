# Verified duplicate proof reclamation

331811334 bytes in three old-format temporary combined archives reclaimed.
Closed SDK identities verified via actual-sdk.json and terminal execution records;
all visible task archive descriptors checked. Canonical pushed manifest byte
identity, every Git part hash/size and concatenated SHA/total verified before
removal. Only temporary proof.tar.gz copies removed; source, SDK, original stores,
media and all canonical Git proof parts retained.

An initial preparation rejected a fourth manifest's different marker names before
any mutation. Corrected code supports the two explicit verified manifest formats;
only the three closed matched archives were reclaimed. Earlier cleanup's
HEAD-versus-remote check also rejected execution while push was still running;
no mutation occurred until push completion was confirmed.
