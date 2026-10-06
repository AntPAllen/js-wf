#!/usr/bin/env python3
"""Independently review a terminal full-domain PostgreSQL projection fixture."""
import argparse
from datetime import datetime
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import re
import subprocess

import fixture_archive

REPO = Path(__file__).resolve().parents[1]
TEST = 'TestPostgresProjectionCrashAndSessionLossFiftyThousandInvocationsInJetStreamDomain'


def require(value, message):
    if not value:
        raise ValueError(message)


def verify_fault(proof, log, killed_log, sdk_hash):
    require(log.rstrip().endswith('PASS') and '--- FAIL:' not in log and '--- SKIP:' not in log,
            'native terminal pass required')
    require(re.findall(r'^--- PASS: (\w+) \([0-9.]+s\)$', log, re.M)==[TEST], 'exact named full case required')
    require(proof['count']==50000 and proof['postgres_fault'] and not proof['native_test_failed'], 'full default count/native success required')
    require(proof['projection_domain']==proof['actual_projection_sdk_domain']=='WFVIEW', 'actual helper and case domain required')
    require(proof['actual_projection_sdk_sha256']==sdk_hash and proof['projection_process_reaped_sigkill'], 'actual matching helper SIGKILL required')
    require(len(proof['actual_projection_sdk_args'])==2 and proof['actual_projection_sdk_args'][1]=='-test.run=^TestProjectionProcessHelper$', 'actual helper command required')
    require(proof['domain_api_requests']>0 and proof['wrong_domain_api_prefix_requests']==0, 'domain request trace required')
    routes = re.findall(r'projection real domain=WFVIEW domain_api_requests=(\d+) wrong_prefix_requests=(\d+)',log)
    require(routes==[(str(proof['domain_api_requests']),'0')], 'actual route log/proof equality required')
    initial = re.findall(r'real domain admitted node=(\d) domain=WFVIEW server_id=(\w+)',log)
    require(sorted(n for n,_ in initial)==['0','1','2'] and len({i for _,i in initial})==3, 'actual initial domain peers required')
    initial = dict(initial)
    healed = proof['healed_domain_peers']
    require(len(healed)==3 and sorted(p['node'] for p in healed)==['0','1','2'] and all(p['domain']=='WFVIEW' for p in healed), 'three actual healed domain peers required')
    healed = {p['node']:p['server_id'] for p in healed}
    require(len(set(healed.values()))==3, 'distinct healed server identities required')
    leader = str(proof['journal_leader_node'])
    require(initial[leader]==proof['journal_leader_old_server_id'] and healed[leader]==proof['journal_leader_new_server_id'] and initial[leader]!=healed[leader], 'actual leader identity replacement required')
    require(all(initial[n]==healed[n] for n in initial if n!=leader), 'unfaulted peers must retain identity')
    require(re.findall(r'projection helper real domain=WFVIEW server_id=(\w+)',killed_log)==[initial['0']], 'killed helper actual domain confirmation required')
    logged = re.findall(r'projection healed domain node=(\d) domain=WFVIEW server_id=(\w+)',log)
    require(len(logged)==3 and dict(logged)==healed, 'healed actual log/proof equality required')
    require(proof['all_results_checked_while_projection_stopped'] and proof['stopped_projection_lag']==100000, 'all results while stopped/100000 lag required')
    require('started 50000 invocations' in log and 'completed and checked 50000 results' in log, 'raw full workload witnesses required')
    require(0<proof['catchup_admitted_rows']<50000 and proof['writer_backend_termination_confirmed'], 'partial catchup and confirmed writer session loss required')
    require(proof['faulted_projection_error'] and proof['final_lag']==0, 'failed writer exit and final lag zero required')
    instant = lambda name: datetime.fromisoformat(proof[name].replace('Z','+00:00'))
    require(instant('projection_process_observed_before_kill')<instant('projection_process_stopped')<instant('run_queue_drained_before_fault')<=instant('all_workflow_workers_joined_before_fault')<instant('fault_started')<instant('journal_leader_stopped')<instant('journal_leader_restarted')<=instant('pinned_clients_refreshed_after_restart'), 'actual fault/lifecycle ordering required')
    for name in ('WF_INV','WF_JRN','KV_WF_STATE','WF_PURGE'):
        require(instant(name+'_all_three_replicas_current')>=instant('faulted_projection_stopped'), 'all source replicas current before replacement required')
    return dict(count=50000,stopped_projection_lag=100000,domain='WFVIEW',initial=initial,healed=healed,
                domain_api_requests=proof['domain_api_requests'],partial_catchup_rows=proof['catchup_admitted_rows'],final_lag=0)


def verify_rows(before, after, expected_hash):
    digest = lambda p: hashlib.file_digest(p.open('rb'),'sha256').hexdigest()
    require(digest(before)==digest(after)==expected_hash, 'complete SQL rows/indexed-column rebuild equality required')
    unique = set(); count = 0
    with before.open() as stream:
        for line in stream:
            typ, ident, status, attrs, data = json.loads(line)
            row = json.loads(data)
            require(typ==row['type']=='view-scale' and ident==row['id']==f'job-{count:05d}' and status==row['status']=='completed', 'exact canonical completed SQL identity required')
            require(json.loads(attrs)=={} and not row.get('attributes') and row['schema_version']==1 and row['last_index']==1, 'exposed row/indexed-column schema required')
            require(row['inv_seq']>0 and row['journal_seq']>0 and row['inv_seq'] not in unique, 'unique acknowledged invocation required')
            require(datetime.fromisoformat(row['updated'].replace('Z','+00:00'))>=datetime.fromisoformat(row['started'].replace('Z','+00:00')), 'projected timestamps required')
            unique.add(row['inv_seq']); count += 1
    require(count==50000, 'all50000 rows required')
    return dict(rows=count,canonical_sha256=expected_hash)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args(); root=args.root.absolute()
    load = lambda name: json.loads((root/name).read_text())
    execution=load('execution.json'); rev=execution['source']
    require(execution['status']=='passed' and execution['exit_code']==0,'actual terminal SDK success required')
    spec=importlib.util.spec_from_file_location('shared',REPO/'scripts/run-domain-runtime-controls.py')
    shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
    before=load('source-before.json');require(before==load('source-after.json') and before['revision']==rev,'before/after source equality required')
    names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=REPO,text=True).splitlines()
    expected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
    require(set(expected)==set(before['files']),'complete selected Git census required')
    data=subprocess.check_output(['git','cat-file','--batch'],cwd=REPO,input=''.join(rev+':'+n+'\n' for n in expected).encode()); stream=io.BytesIO(data)
    for name in expected:
        header=stream.readline().split();require(header[1]==b'blob','Git blob required');body=stream.read(int(header[2]));require(stream.read(1)==b'\n','Git body boundary required')
        require(hashlib.sha256(body).hexdigest()==before['files'][name]==shared.sha(root/'selected-source'/name),'exact retained/Git source required')
    require(not stream.read(),'Git batch census end required')
    external=load('external-source-before.json');require(external==load('external-source-after.json'),'external source before/after equality required')
    paths=load('external-captured-paths.json');require(set(paths)==set(external),'complete external selected paths required')
    for name,digest in external.items():require(shared.sha(root/paths[name])==digest,'retained external input hash required')
    sdk=load('actual-sdk.json');binary=load('binary.json');commands=load('commands.json')
    require(sdk['args']==commands['test']==[str(root/'integration.test'),'-test.run=^'+TEST+'$','-test.count=1','-test.v','-test.timeout=22m'],'original exact parent SDK command required')
    require(sdk['exe_sha256']==binary['sha256']==shared.sha(root/'integration.test') and sdk['exe']==str(root/'integration.test'),'actual parent executable required')
    info=binary['build_info'];require('vcs.revision='+rev in info and 'vcs.modified=false' in info and '-race=true' not in info and re.search(r'github.com/nats-io/nats-server/v2\s+v2\.15\.0\s',info),'clean source/nonrace/pinned server SDK required')
    require(not Path('/proc',str(sdk['pid'])).exists(),'actual parent closure required')
    require(sdk['working_directory']==commands['working_directory']==str(REPO),'actual package launch directory required')
    require(sdk['environment']==commands['environment'] and sdk['environment']['GOMAXPROCS']=='2' and sdk['environment']['GOMEMLIMIT']=='2GiB' and sdk['environment']['GOWORK']=='off' and sdk['environment']['GOFLAGS']=='' and not sdk['actual_WF_PROJECTION_COUNT_present'],'original profile and no count override required')
    fixtures=list((root/'originals').rglob('projection-fault-proof.json'));require(len(fixtures)==1,'exact retained original fixture required');case=fixtures[0].parent
    proof=json.loads(fixtures[0].read_text());fault=verify_fault(proof,(root/'native.log').read_text(),(case/'killed-projection.log').read_text(),binary['sha256'])
    require(not Path('/proc',str(proof['actual_projection_sdk_pid'])).exists(),'actual helper closure required')
    require(proof['actual_projection_sdk_build_info'].splitlines()[1:]==info.splitlines()[1:],'actual helper SDK build identity required')
    rows=verify_rows(case/'before-rebuild.jsonl',case/'after-rebuild.jsonl',proof['row_and_indexed_column_sha256'])
    trace=json.loads((case/'projection-dependency-trace.json').read_text())
    require(trace['counts']['WF_INV.OrderedConsumer']['started']>=2 and trace['counts']['WF_INV.Messages']['started']>=2 and trace['counts']['WF_INV.Next']['started']>0 and 'WF_INV.Fetch' not in trace['counts'],'actual continuous invocation iterator required')
    pg=load('actual-postgres.json');stopped=load('postgres-stopped.json');media=load('postgres-media-copy-verification.json')
    require(pg['actual_executable_sha256']==shared.sha(root/'actual-postgres') and not stopped['State']['Running'] and stopped['State']['Pid']==0 and stopped['Id']==pg['container']['Id'] and media['all_closed_sql_media_bytes_match'],'actual PostgreSQL executable/owned stopped container required')
    for name,digest in media['files'].items():require(shared.sha(root/'postgres-stopped-data'/name)==digest,'all closed SQL media copies required')
    closure=shared.closure(root)
    proofdir=root.with_name(root.name+'-proof');meta=json.loads((proofdir/'archive-verification.json').read_text());manifest=json.loads((proofdir/'fixture-inventory.json').read_text())
    require(fixture_archive.verify(root.with_suffix('.tar.gz'))==manifest and fixture_archive.inventory(root)==manifest['files'],'full original archive members/current census required')
    with root.with_suffix('.tar.gz').open('rb') as stream:
        require(fixture_archive.digest(stream)==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']),'complete compressed archive hash required')
    report=dict(source=rev,selected_git_inputs=len(expected),selected_external_inputs=len(external),actual_sdk=sdk,execution=execution,fault=fault,rows=rows,closed_sql_media_files=len(media['files']),complete_archive=meta,fresh_closure=closure,
                scope='Full50000 PostgreSQL/domain projection SIGKILL, writer session loss, library journal restart and exact exposed-row/index rebuild. No NATS process SIGKILL, independent copied-store audit, leaf, natural reply loss, fullmatrix, million drain or actual24h qualification.')
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(dict(source=rev,rows=50000,archive_members=meta['members'])))


if __name__=='__main__':
    main()
