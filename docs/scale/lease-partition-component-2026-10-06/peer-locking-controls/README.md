# Focused upstream test setup locking controls

At `bbf33a0`, upstream and contiguous race binaries each pass the two original tests with ten original subcases. Eight lock/unlock pairs protect 17 `addPeer` calls, following the method's documented locking contract. Transformation reversal proves all other original file bytes, assertions, cases, sleeps and deadlines are unchanged. Original production code is unchanged on upstream; the contiguous production overlay exactly matches the previously qualified guard. The two cases retain count1/3m and actual 2-CPU/2-GiB environments.

Independent review verifies 2,009 selected Git/current/retained/before/after inputs, 2,664 compiled dependency files, 598 unchanged original module files, both actual live SDK identities/argv/environment/birth, all twelve native RUN/PASS records per profile, zero skips/races and the complete 5,305-member archive. Archive: 88,794,486 bytes, SHA256 `0a1089b47ae1b870e0c9eccf33e6ad4e63088507f197c6d1b554c2866f65c730`.

This qualifies a focused test setup correction. The original refined170 native failure remains failed; a complete 170-case execution with this correction is separate. Production dependency adoption, workflow matrix, seeded workflow Tier1 and 24-hour gates are not promoted.
