#!/usr/bin/env python3
"""Join retained fencing, exact deliveries and terminal timestamps without inferring server causes."""
import argparse
from collections import Counter
import gzip
import importlib.util
import json
from pathlib import Path

spec=importlib.util.spec_from_file_location('row_guard',Path(__file__).with_name('check-tier3-journal-row.py'))
row=importlib.util.module_from_spec(spec)
spec.loader.exec_module(row)
ns=row.timestamp_ns


def read(root,name):
    path=root/name
    if path.exists():return json.loads(path.read_bytes())
    return json.loads(gzip.decompress((root/(name+'.gz')).read_bytes()))


def review(fencing,dispatch,latencies,faults):
    records=[]
    for event in fencing or []:
        at=ns(event['At'])
        same=lambda x:x.get('Type',x.get('type'))==event['Type'] and x.get('ID',x.get('id'))==event['ID']
        steps=[x for x in dispatch or [] if same(x) and x['Worker']==event['Worker'] and x['RunSequence']==event['RunSequence'] and x['Delivery']==event['Delivery']]
        fetched=[x for x in steps if x['Stage']=='fetched']
        terminal=[x for x in latencies if same(x) and x['event']=='terminal']
        if len(fetched)!=1 or len(terminal)!=1:
            raise ValueError('fencing lacks one exact delivery fetch or one invocation terminal sample')
        fetch=ns(fetched[0]['At']); completed=ns(terminal[0]['observed'])
        if fetch>at:raise ValueError('fencing precedes its delivery fetch')
        if completed<=fetch:classification='already_terminal_before_fetch'
        elif completed<at:classification='completed_during_original_delivery'
        else:classification='completed_after_fencing'
        overlaps=[i+1 for i,f in enumerate(faults) if ns(f['killed'])<=at<=ns(f['healed'])]
        pauses=[i+1 for i,f in enumerate(faults) if f.get('worker')==event['Worker'] and f.get('paused_leases') and ns(f['paused'])<=completed<ns(f['resumed']) and any(l['key']==event['Type']+'.'+event['ID'] and l['epoch']==event['Epoch'] for l in f['paused_leases'])]
        if pauses:
            if classification!='completed_during_original_delivery':raise ValueError('paused ownership timeline disagrees with original delivery')
            classification='completed_while_original_owner_stopped'
        acks=[x for x in dispatch or [] if same(x) and x['Stage']=='ack' and ns(x['At'])>=at]
        records.append(dict(event=event,classification=classification,overlapping_faults=overlaps,
                            matching_paused_ownership=pauses,exact_delivery_steps=steps,terminal_sample=terminal[0],
                            later_invocation_ack_observations=acks,
                            later_exact_delivery_ack_observations=[x for x in acks if x['Worker']==event['Worker'] and x['RunSequence']==event['RunSequence'] and x['Delivery']==event['Delivery']],
                            server_root_cause_confirmed=False))
    return dict(scope='retained delivery and terminal timelines',fencing_records=len(records),
                classifications=dict(Counter(r['classification'] for r in records)),
                outside_confirmed_faults=sum(not r['overlapping_faults'] for r in records),
                records=records,ack_observations_prove_broker_commit=False,
                independently_reaudits_server_state=False,clears_full_tier3_release=False)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    result=review(*(read(args.root,n) for n in ('fencing.json','dispatch.json','latencies.json','faults.json')))
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k!='records'},indent=2))
