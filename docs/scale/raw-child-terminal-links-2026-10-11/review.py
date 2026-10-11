import hashlib, json, re
from pathlib import Path
import yaml
out = Path(__file__).resolve().parent
manifest = json.loads((out/'source-inputs.json').read_text())
for name, sha in manifest['files'].items():
    assert hashlib.sha256(Path(name).read_bytes()).hexdigest() == sha, name
commands = {}
for name in ('command.json', 'native-command.json'):
    receipt = json.loads((out/name).read_text())
    assert receipt['actual_exit_code'] == 0 and receipt['executable_captured'], name
    commands[name] = (receipt['end_ns']-receipt['start_ns'])/1e9
race = (out/'race.log').read_text()
native = (out/'native.log').read_text()
workflow = yaml.safe_load(Path('.github/workflows/graph-publication.yml').read_text())
guard = None
for job in workflow['jobs'].values():
    for step in job.get('steps', []):
        script = step.get('run', '')
        if "assert '--- PASS: TestRawGraphChildTerminalLinks ' in log" in script:
            start = script.index("assert '--- PASS: TestRawGraphChildTerminalLinks ' in log")
            end = script.index("assert '--- PASS: TestRawGraphChildStartLinks ' in log", start)
            guard = script[start:end].rstrip()
assert guard
exec(guard, {'log': race})
assert native.count('RAW_SDK_CHILD_TERMINAL_LINKS verified=1 unavailable=0') == 4
assert native.count('RAW_SDK_CHILD_START_LINKS verified=1 unavailable=0') == 4
assert native.count('RAW_SDK_CHECKPOINT_HISTORY checkpoints=2') == 4
assert '\nFAIL' not in race and '\nFAIL' not in native
result = {'source_inputs_unchanged': len(manifest['files']), 'new_physical_controls':22,
          'new_positive_controls':10, 'new_negative_controls':12,
          'native_child_terminal_links_verified':4, 'native_unavailable':0,
          'native_checkpoint_frames':8, 'exact_new_ci_guard_executed':True,
          'raw_graph_top_level_passes':sum(line.startswith('--- PASS: TestRawGraph') for line in race.splitlines()),
          'race_test_seconds':float(re.search(r'ok\s+js-wf/integrity\s+([\d.]+)s', race).group(1)),
          'native_test_seconds':float(re.search(r'ok\s+js-wf/worker\s+([\d.]+)s', native).group(1)),
          'driver_seconds':commands,
          'scope':'Exact available retained same-generation projection bytes, not original lost terminal history, publication/lease causality or old generations'}
(out/'review.json').write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps(result))
