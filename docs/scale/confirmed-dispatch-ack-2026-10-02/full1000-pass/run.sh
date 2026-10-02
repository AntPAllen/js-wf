#!/bin/bash
set -euo pipefail
cd /home/exedev/js-wf
root=/tmp/js-wf-confirmed-ack-boundary-full1000-20261002
git rev-parse HEAD > "$root/source.txt"
SIM_COVERAGE_SUMMARY=0 go test -p=1 ./sim -list '^Test' > "$root/list.log"
rg '^Test[[:alnum:]_]+$' "$root/list.log" > "$root/inventory.txt"
go run -p=1 ./scripts/tier1-seed-inventory > "$root/seeded-inventory.txt"
rg --files sim/testdata/regressions | sort > "$root/regression-inventory.txt"
/usr/bin/time -o "$root/time.txt" -f 'elapsed=%e user=%U system=%S' go test -p=1 -json ./sim -count=1 -timeout=10m > "$root/events.jsonl"
python3 scripts/check-tier1-suite.py --events "$root/events.jsonl" --inventory "$root/inventory.txt" --regressions "$root/regression-inventory.txt" --source "$root/source.txt" --seeded-inventory "$root/seeded-inventory.txt" --seeds 1000 --output "$root/result.json"
