"""Reproduce the supplementary audit timing and actual SDK build-info checks.

The complete archive/member/source review is separate in review.py. This command
only verifies the three failed attempts and the recorded actual build settings.
"""
import argparse
from datetime import datetime
import json
from pathlib import Path
import subprocess

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--root',type=Path,required=True)
a=p.parse_args();root=a.root.resolve()
audits=json.loads((root/'fixture/checkpoint-audits.json').read_text());audit=audits[-1]
assert audit['batch']==110 and audit['invocation_cutoff']==3080
assert audit['error']=='retained audit: context deadline exceeded' and len(audit['attempts'])==3
elapsed=[]
for attempt in audit['attempts']:
 assert attempt['error']=='context deadline exceeded'
 assert attempt['report']==dict(Invocations=3080,Journals=0,Entries=0,Terminal=0)
 seconds=(datetime.fromisoformat(attempt['completed'])-datetime.fromisoformat(attempt['started'])).total_seconds()
 assert abs(seconds-20)<.1
 elapsed.append(seconds)
total=(datetime.fromisoformat(audit['completed'])-datetime.fromisoformat(audit['started'])).total_seconds()
assert abs(total-60)<.1
recorded=json.loads((root/'binary.json').read_text())['build_info'].splitlines()
actual=subprocess.check_output(['go','version','-m',str(root/'integration.test')],text=True).splitlines()
assert recorded[0].split(': ',1)[1]==actual[0].split(': ',1)[1]
assert recorded[1:]==actual[1:]
print(json.dumps(dict(all_three_timed_out_attempts_verified=True,attempt_seconds=elapsed,total_seconds=total,actual_binary_all_build_info_fields_match=True),indent=2))
