import ast
import json
from pathlib import Path
import subprocess
import tempfile

base = Path(__file__).resolve().parent
repo = base.parents[2]
source = repo / 'scripts/check-tier1-race.py'
module = ast.parse(source.read_text())
function = next(node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == 'call')
with tempfile.TemporaryDirectory(prefix='js-wf-command-exit-control-') as name:
    root = Path(name)
    scope = {'REPO': repo, 'root': root, 'commands': [], 'execution_contexts': [], 'command_results': [], 'subprocess': subprocess, 'json': json, 'env': {}}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), 'exec'), scope)
    scope['call'](['/bin/true'])
    try:
        scope['call'](['/bin/false'])
    except subprocess.CalledProcessError as error:
        assert error.returncode == 1
    else:
        raise AssertionError('failed command was accepted')
    results = json.loads((root / 'command-results.json').read_text())
    assert [row['exit_code'] for row in results] == [0, 1]
    assert all(row['working_directory'] == str(repo) for row in results)
    (base / 'control-results.json').write_text(json.dumps(results, indent=2) + '\n')
assert '-test.timeout=300m' in source.read_text()
assert 'SIM_SEEDS=str(a.seeds)' in source.read_text()
print('PASS actual successful/failed child exits retained; failure propagated')
