import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
import zipfile
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('tier2_full_row', Path(__file__).with_name('review-tier2-row.py'))
row = importlib.util.module_from_spec(spec)
spec.loader.exec_module(row)


def fixture():
    revision = 'a'*40
    run = dict(id=10, head_sha=revision, status='completed', conclusion='success',
               event='workflow_dispatch', path='.github/workflows/tier2-matrix-journal.yml')
    job = lambda n, name: dict(id=n, run_id=10, head_sha=revision, name=name,
                              status='completed', conclusion='success')
    artifact = lambda n, name: dict(id=n, name=name, expired=False, digest='sha256:'+'b'*64,
                                   workflow_run=dict(id=10, head_sha=revision))
    generator = job(20, 'seeds')
    seeds = [dict(seed=n, job=job(100+n, f'leader ({n})'),
                  artifact=artifact(1000+n, f'matrix-partition-{n}-10m'),
                  source_artifact=artifact(2000+n, f'workload-source-partition-{n}'),
                  log=revision+f'\nSustained matrix row=partition seed={n} duration=10m')
             for n in range(1, 201)]
    return run, generator, seeds


class FullRowTests(unittest.TestCase):
    def test_exact_200_binding_and_failures(self):
        run, generator, seeds = fixture()
        row.bind(run, generator, seeds, 10, 'a'*40, 'partition')
        cases = [('run', 'status', 'queued'), ('run', 'conclusion', 'failure'),
                 ('run', 'head_sha', 'c'*40), ('run', 'id', 11),
                 ('generator', 'conclusion', 'failure'), ('generator', 'head_sha', 'c'*40),
                 ('job', 'id', True), ('job', 'head_sha', 'c'*40),
                 ('job', 'conclusion', 'failure'), ('job', 'name', 'leader (partition, 1)'),
                 ('artifact', 'expired', True), ('source_artifact', 'name', 'workload-source-journal-1')]
        for target, key, value in cases:
            r, g, s = fixture()
            objects = dict(run=r, generator=g, **{k:s[0][k] for k in ('job','artifact','source_artifact')})
            objects[target][key] = value
            with self.subTest(target=target,key=key), self.assertRaises(ValueError):
                row.bind(r,g,s,10,'a'*40,'partition')
        for changes in ('missing','duplicate','bool','duration','row','duplicate_job','duplicate_artifact'):
            r,g,s = fixture()
            if changes == 'missing':s.pop()
            elif changes == 'duplicate':s[-1] = copy.deepcopy(s[0])
            elif changes == 'bool':s[0]['seed'] = True
            elif changes == 'duration':s[0]['log'] = s[0]['log'].replace('10m','35s')
            elif changes == 'row':s[0]['log'] = s[0]['log'].replace('partition','journal')
            elif changes == 'duplicate_job':s[1]['job']['id'] = s[0]['job']['id']
            else:s[1]['source_artifact']['id'] = s[0]['artifact']['id']
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                row.bind(r,g,s,10,'a'*40,'partition')

    def test_complete_provider_listings_required(self):
        _,g,s = fixture()
        jobs = dict(total_count=201,jobs=[g]+[i['job'] for i in s])
        artifacts = dict(total_count=400,artifacts=[a for i in s for a in (i['artifact'],i['source_artifact'])])
        row.check_listings(jobs,artifacts,g,s)
        for change in ('missing','extra','duplicate','substituted'):
            j = copy.deepcopy(jobs)
            if change == 'missing':j['jobs'].pop()
            elif change == 'extra':j['jobs'].append(g)
            elif change == 'duplicate':j['jobs'][-1]=g
            else:j['jobs'][-1]['head_sha']='c'*40
            with self.subTest(change=change), self.assertRaises(ValueError):
                row.check_listings(j,artifacts,g,s)
        bad=copy.deepcopy(artifacts);bad['artifacts'][-1]['digest']='sha256:'+'c'*64
        with self.assertRaises(ValueError):row.check_listings(jobs,bad,g,s)

    def test_provider_zip_digest_members_and_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'raw.zip'
            with zipfile.ZipFile(p,'w') as z:z.writestr('evidence.json',b'original')
            artifact=dict(digest='sha256:'+hashlib.sha256(p.read_bytes()).hexdigest())
            self.assertEqual(row.zip_inventory(p,artifact), {'evidence.json':hashlib.sha256(b'original').hexdigest()})
            extracted=Path(tmp)/'raw';extracted.mkdir();(extracted/'evidence.json').write_bytes(b'original')
            row.check_provider(p,artifact,extracted)
            (extracted/'evidence.json').write_bytes(b'mutated')
            with self.assertRaises(ValueError):row.check_provider(p,artifact,extracted)
            with self.assertRaises(ValueError):row.zip_inventory(p,dict(digest='sha256:'+'0'*64))
            for name in ('../escape','/absolute','dir\\escape'):
                with zipfile.ZipFile(p,'w') as z:z.writestr(name,b'bad')
                artifact['digest']='sha256:'+hashlib.sha256(p.read_bytes()).hexdigest()
                with self.subTest(name=name),self.assertRaises(ValueError):row.zip_inventory(p,artifact)

    def test_git_source_selection_matches_real_materializer(self):
        spec=importlib.util.spec_from_file_location('materializer',Path(__file__).with_name('materialize-workload-source.py'))
        materializer=importlib.util.module_from_spec(spec);spec.loader.exec_module(materializer)
        with tempfile.TemporaryDirectory() as tmp:
            checkout=Path(tmp)/'git';checkout.mkdir()
            git=lambda *args:subprocess.check_output(['git',*args],cwd=checkout)
            git('init','-q')
            for name in ('go.mod','worker/asset.bin','scripts/space [1].txt','docs/proof/check.py','docs/proof/archive.bin'):
                p=checkout/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(name)
            git('add','.');git('-c','user.name=Test','-c','user.email=test@example.invalid','commit','-qm','Fixture')
            revision=git('rev-parse','HEAD').decode().strip()
            actual=materializer.materialize(checkout,Path(tmp)/'source.json')
            expected=row.expected_source(revision,checkout)
            row.check_source(actual,expected)
            self.assertNotIn('docs/proof/archive.bin',expected['inputs'])
            for mutate in ('missing','hash','bool'):
                bad=copy.deepcopy(actual)
                if mutate=='missing':bad['inputs'].pop('worker/asset.bin')
                elif mutate=='hash':bad['inputs']['worker/asset.bin']['sha256']='0'*64
                else:bad['omitted_docs_paths']=True
                with self.subTest(mutate=mutate),self.assertRaises(ValueError):row.check_source(bad,expected)

    def test_collection_path_and_missing_raw_cannot_qualify(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'root';root.mkdir()
            for name in ('../escape','/absolute','a/../b'):
                with self.assertRaises(ValueError):row.input_path(root,name)
            (root/'link').symlink_to('/dev/null')
            with self.assertRaises(ValueError):row.input_path(root,'link')
            with self.assertRaises(ValueError):row.review(root,10,'a'*40,'partition')
            (root/'link').unlink()
            with self.assertRaises(FileNotFoundError):row.review(root,10,'a'*40,'partition')

    def test_collector_pagination_and_fresh_extraction_guards(self):
        spec=importlib.util.spec_from_file_location('collector',Path(__file__).with_name('collect-tier2-row.py'))
        collector=importlib.util.module_from_spec(spec);spec.loader.exec_module(collector)
        with patch.object(collector,'api',return_value=[dict(total_count=2,jobs=[{'id':1}]),dict(total_count=2,jobs=[{'id':2}])]):
            self.assertEqual(len(collector.listing('path','jobs')['jobs']),2)
        for pages in ([dict(total_count=2,jobs=[{'id':1}])],
                      [dict(total_count=2,jobs=[]),dict(total_count=3,jobs=[])]):
            with patch.object(collector,'api',return_value=pages),self.assertRaises(ValueError):
                collector.listing('path','jobs')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'queued'
            with patch.object(collector,'api',return_value=dict(id=10,head_sha='a'*40,status='queued',conclusion='')):
                with self.assertRaises(ValueError):collector.collect(root,10,'a'*40,'partition')
            self.assertFalse(root.exists())
            archive=Path(tmp)/'source.zip'
            with zipfile.ZipFile(archive,'w') as z:z.writestr('workload-source.json','{}')
            artifact=dict(digest='sha256:'+hashlib.sha256(archive.read_bytes()).hexdigest())
            destination=Path(tmp)/'source'
            collector.extract_verified(archive,artifact,destination)
            with self.assertRaises(ValueError):collector.extract_verified(archive,artifact,destination)
            self.assertEqual((destination/'workload-source.json').read_text(),'{}')


if __name__ == '__main__':
    unittest.main()
