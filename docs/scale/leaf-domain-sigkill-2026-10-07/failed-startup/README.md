# First SIGKILL-profile attempt failed during startup

Source8389fe4, original race SDK/native exit1 after1.63s: R3 provisioning returns API10005 `no suitable peers for placement`. The leaf was constructed and its log records a domain connection, but no manifest cut or SIGKILL executed. No completed leaf proof or positive fault result exists. The brief leaf process was not independently observed by the periodic producer; its logged PID is not process identity admission. SDK buildVCS/actual hash/argv/environment and selected source/Git/before/after plus every complete archive member are independently reviewed. Accepted=false; the original failure stays unchanged.

A metadata-readiness race is consistent with early placement failure, but the failed attempt did not record hub membership at the failing request, so exact cause is unconfirmed. The next fresh fixture explicitly requires an elected/current hub metadata leader with fresh stats naming all three configured peers before R3 provisioning. Semantic placement failures remain fatal after that gate; no global retry relaxation or deadline/count reduction. No failed store is reopened.

Local completed fixture and staging archive retired after fresh full remote/member/local-inventory/closure verification. Complete bytes remain in S3; [removal/restoration records](../reclaimed/README.md).
