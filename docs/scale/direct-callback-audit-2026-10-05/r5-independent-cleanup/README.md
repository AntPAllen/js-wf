# Independent cleanup accepted; restart overlap remains failed

At7d79d13 native race parentFAIL52.73s. Owner-left-down fixturePASS27.61s overall,
exact1500/6000 audit; zero INV/JRN consumer observations at4.056609/4.057684s.
Actual old-owner deletion times out; independent replacement deletion succeeds.
Both retain original20s audit and2s cleanup ceiling. This qualifies the focused
left-down schedule and confirms the deletion-starvation correction, not the parent.

Same-store restart fails1.392990s after1961accepted records on sequence1 replay.
Actual same-name/created/owner cursor Info changes delivery and AckNone floor
from1961/1961 before kill to1126/1126 at overlap, pending4039→4874. This confirms
volatile position regression while assignment identity/config survives. Existing
reader rejects it; original failure remains preserved.

Independent selectedGit/actualSDK/actual external servers/module/mounts/closure
and full1666-member69,304,739byte archive/readback verified, SHA256
`ea8fabd63a7db03e797eb28001b33c8fcf882f454e40f3ae68abde46e0e37723`.
Next candidate admits only fresh API-confirmed R1 memory/AckNone position loss
behind already accepted data under identical config/created/owner. Fresh cursor
starts at unvisited sequence; no overlap reaches visitor. No blanket replay,
large-fault/live/default/finalmatrix/24h claim.
