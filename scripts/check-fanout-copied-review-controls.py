#!/usr/bin/env python3
"""Reject malformed evidence using a closed copied case without changing stores."""
import argparse
import copy
import importlib.util
import json
from pathlib import Path
from unittest.mock import patch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    spec = importlib.util.spec_from_file_location('copied_controls', Path(__file__).with_name('run-fanout-copied-audit.py'))
    reviewer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reviewer)
    actual_read = reviewer.read
    reviewer.require(reviewer.review(root) == actual_read(root / 'independent-review.json'), 'valid baseline review changed')
    controls = []

    def check(label, filename, mutate):
        data = copy.deepcopy(actual_read(root / filename))
        mutate(data)

        def altered(path):
            return copy.deepcopy(data) if path == root / filename else actual_read(path)

        rejected = False
        with patch.object(reviewer, 'read', side_effect=altered):
            try:
                reviewer.review(root)
            except (ValueError, KeyError, TypeError):
                rejected = True
        controls.append(dict(control=label, rejected=rejected))
        reviewer.require(rejected, 'malformed evidence accepted: ' + label)

    check('boolean_audit_duration', 'retained-review.json', lambda data: data.update(audit_ns=True))
    check('omitted_selected_build_input', 'selected-inputs.json', lambda data: data['files'].pop(next(iter(data['files']))))
    result = dict(valid_baseline_verified=True, controls=controls,
                  scope='Virtual metadata corruptions only; no store modification, NATS startup or native rerun')
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
