#!/bin/bash
set -euo pipefail
cd /home/exedev/js-wf
proof_root=/tmp/js-wf-tier1-portable-20261001
mkdir -p "$proof_root"
git rev-parse HEAD > "$proof_root/source.txt"
export SIM_SEEDS=1000 SIM_COVERAGE_SUMMARY=1 GOMEMLIMIT=512MiB GOMAXPROCS=2
export FAULT_TRACE_OUT="$proof_root/failure.json"
SIM_COVERAGE_SUMMARY=0 /usr/local/bin/go test -p=1 ./sim -list '^Test' > "$proof_root/list.log"
rg '^Test[[:alnum:]_]+$' "$proof_root/list.log" > "$proof_root/inventory.txt"
rg --files sim/testdata/regressions -g '*.json' | sort > "$proof_root/regression-inventory.txt"
/usr/bin/time -o "$proof_root/time.txt" -f 'elapsed=%e user=%U system=%S' /usr/local/bin/go test -p=1 -json ./sim -count=1 -timeout=8m | tee "$proof_root/events.jsonl" | python3 -u scripts/render-go-test.py > "$proof_root/suite.log"
python3 scripts/check-tier1-suite.py --events "$proof_root/events.jsonl" --inventory "$proof_root/inventory.txt" --regressions "$proof_root/regression-inventory.txt" --source "$proof_root/source.txt" --seeds "$SIM_SEEDS" --output "$proof_root/result.json"
