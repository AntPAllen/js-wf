"""Bind the recorded explicit parallel profile to actual SDK selector output."""
import re


def validate(state, environment, events):
    selected = state.get('parallel_state_retained_audit', False)
    outputs = [e.get('Output', '') for e in events]
    markers = [re.fullmatch(r'MATRIX_AUDIT_MODE mode=parallel_state cutoff=(full|0|[1-9][0-9]*)\n', output)
               for output in outputs if output.startswith('MATRIX_AUDIT_MODE')]
    assert all(markers), 'malformed selector evidence'
    assert environment.get('WF_TIER3_PARALLEL_STATE_RETAINED_AUDIT') == ('1' if selected else None)
    if not selected:
        assert not markers, 'parallel selector ran without recorded opt-in'
        return dict(selected=False)
    assert state['memory_limit'] == environment['GOMEMLIMIT'] == '4GiB'
    assert state['gomaxprocs'] == environment['GOMAXPROCS'] == '4'
    assert state['gc_percent'] == environment['GOGC'] == '500'
    for name in ('BATCHED', 'STREAMING_STATE', 'CONCURRENT_STATE', 'CHUNKED_STATE'):
        assert 'WF_TIER3_' + name + '_RETAINED_AUDIT' not in environment
    assert any(m[1] == 'full' for m in markers), 'final full audit did not select candidate'
    assert any(m[1] != 'full' and int(m[1]) > 0 for m in markers), 'checkpoint audit did not select candidate'
    return dict(selected=True, selector_markers=len(markers),
                checkpoint_selections=sum(m[1] != 'full' for m in markers),
                full_selections=sum(m[1] == 'full' for m in markers),
                scope='Recorded profile and actual selector output only; original sustained raw row/provenance gates still required.')
