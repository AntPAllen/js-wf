# Named capacity cleanup diagnostic

Previous acc9123 exact400k/4.8M read/reduction completed17.114920s, but JRNcount2
persisted to20s despite both created cursor Delete success. Identity/cause remains
unconfirmed. Donor metadata includes historical audit names, not proof of active
restored consumers.

Changed WF_AUDIT_CAPACITY_CURSOR_NAMES=1 captures full named configurations before
creating any current audit cursor. At the first nonzero post-delete count it lists
named consumers and queries every current audit cursor by name, recording typed
not-found separately. Post-delete probes stay inside original20s. No pre-existing
consumer is deleted and zero count remains required. Compile/opt-in skip verified;
fresh full400k/4.8M copy execution pending after preserving/reclaiming prior copy.
