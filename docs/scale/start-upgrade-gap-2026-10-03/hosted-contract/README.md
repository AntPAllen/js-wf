# Hosted Start crash across retained-store upgrade

Run [37122997232](https://github.com/AntPAllen/js-wf/actions/runs/37122997232)
uses exact source `cc0fcd189880c561cb0ba7a90a0dde581586bb46`.
Independent review verifies all 1,287 original archive members against SHA256,
all 715 clean before/after source hashes against Git, and actual embedded Go
build settings from both retained race binaries.

Both mixed-version profiles pass: confirmed client SIGKILL to verified terminal
is 7.502384102 s and 7.519410059 s, below the explicit 30 s bound. The full
positive package passes in 31.971 s. The precise compiled omission of gap
repair fails at its required retained dispatch/journal assertion in 15.374 s.
Raw invocation bytes remain unchanged through the retained-store upgrade;
terminal identity and duplicate Start checks pass.

The original compressed archive is split into numbered byte parts. Concatenate
parts in numeric order to `start-upgrade-gap-originals.tar.gz` in a temporary
directory, copy the manifest and terminal JSON there, then run:

```sh
python3 review.py /path/to/restored-originals /path/to/js-wf
```

Each part and their concatenation were SHA256 checked before commit. The
reviewer streams every original member and reads each real binary's build
settings; full store extraction is unnecessary. This accepts the R3 contract,
not sustained R5 forced-gap coverage, the 200-seed matrices or the 24-hour soak.
