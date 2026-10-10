# Phase 4 direct-call linter coverage

The original scanner ignored dot imports and only matched a bare selector as
the call target. Consequently it missed `*Context` from a dot-imported workflow
package, `Now()` from dot-imported time, and generic/parenthesized calls such as
`rand.N[int](10)` and `(time.Now)()`.

The [original-source overlay](negative-overlay.json) with the
[valid five-case corpus](negative-corpus.go.txt) fails all five cases
[before the fix](valid-corpus-before-fix.log), with semantic missing-findings
failures and no build failure. The original scanner itself is retained as
`original-lint.go.txt`. Earlier draft development evidence is also retained;
the valid corpus is the authoritative negative control.

The scanner now recognizes dot-imported workflow contexts and standard time/
random functions, unwraps parentheses and generic instantiation at direct call
sites, and handles both named and dot-imported journaled Run/RunOnce callbacks.
Local shadowing and effect exclusions remain asserted. A toolchain source
check requires the static dot-random function table to cover all current
exported functions; the shipped binary does not need a GOROOT source tree.

[Race checks](race-with-export-guard.log) pass for the complete linter and
command packages, including the five-case corpus and export-table guard.
[Actual compiled command](compiled-command.json) reports the formerly missed
parenthesized clock/generic random calls and exits 1 with the exact expected
diagnostics. Its source and executable hashes are recorded. These are focused
development checks, not clean-source workflow/runtime release qualification.
The existing normal CI package suite includes both linter packages.

The linter remains a direct source scan. It does not resolve separate helpers,
indirect function values or a call graph. Workflow runtime behavior, public
admission and collection are unchanged; the original broader gates remain open.
