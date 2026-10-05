# Independent cursor cleanup within the original deadline

The common scanner now issues up to three independent cursor deletions
concurrently and joins them under its unchanged2s ceiling and original caller
deadline. An unanswered old-owner deletion cannot starve a replacement deletion.
Unit race regression with an unavailable owner and two replacements passes;
control fake deletion bookkeeping now uses a mutex. Initial race run correctly
reported that fake's unsynchronized writes; revised controls pass1.209s.

Fresh actualR5 SIGKILL/same-store fixtures retain1500/6000 and20s, capture actual
per-name delete replies/times alongside cursor Info and cleanup observations.
Native result pending. Existing replay/retry/default semantics unchanged; prior
failed native parents remain failed and preserved. This candidate still does not
qualify unexplained same-owner replay or large fault capacity/live/24h.
