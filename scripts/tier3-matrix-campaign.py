#!/usr/bin/env python3
"""Plan bounded R5 row campaigns within the hosted 256-job limit."""
import importlib.util
import json
import os
from pathlib import Path

spec = importlib.util.spec_from_file_location('tier3_rows', Path(__file__).with_name('check-tier3-journal-row.py'))
rows = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rows)
ROWS = tuple(rows.TESTS)


def campaign(row, count, first=1, require_clock=False):
    if row not in (*ROWS, 'all') or type(count) is not int or count not in (1, 20, 200):
        raise ValueError('unsupported row or seed count')
    if type(first) is not int or first < 1 or first + count - 1 > 2**63 - 1:
        raise ValueError('invalid positive int64 seed range')
    if require_clock and row != 'all' and not row.startswith('server_clock_'):
        raise ValueError('clock proof flags require clock rows or the full matrix')
    width = 12 if row == 'all' else 1
    jobs = [dict(row=selected, first=start, last=min(start+width-1, first+count-1),
                 artifact_seed=str(start) if start == min(start+width-1, first+count-1)
                 else f'{start}-{min(start+width-1, first+count-1)}')
            for selected in (ROWS if row == 'all' else (row,))
            for start in range(first, first+count, width)]
    if len(jobs) > 256:
        raise ValueError('campaign exceeds hosted matrix limit')
    return jobs


if __name__ == '__main__':
    jobs = campaign(os.environ['MATRIX_ROW'], int(os.environ['SEED_COUNT']), int(os.environ['START_SEED']),
                    os.environ.get('CLOCK_TIMER_CUT') == '1' or os.environ.get('COMMON_TIMER_CLOCK') == '1')
    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
        output.write('jobs=' + json.dumps(jobs) + '\n')
