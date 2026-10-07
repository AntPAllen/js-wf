import copy
import unittest
import parallel_soak_profile


class ParallelSoakProfile(unittest.TestCase):
    def baseline(self):
        return ({'parallel_state_retained_audit':True,'memory_limit':'4GiB','gomaxprocs':'4','gc_percent':'500'},
                {'WF_TIER3_PARALLEL_STATE_RETAINED_AUDIT':'1','GOMEMLIMIT':'4GiB','GOMAXPROCS':'4','GOGC':'500'},
                [{'Output':'MATRIX_AUDIT_MODE mode=parallel_state cutoff=28\n'},
                 {'Output':'MATRIX_AUDIT_MODE mode=parallel_state cutoff=full\n'}])

    def test_actual_selections_and_budget_required(self):
        state, env, events = self.baseline()
        self.assertEqual(parallel_soak_profile.validate(state, env, events)['selector_markers'],2)
        for mutate in (
            lambda s,e,v:s.update(parallel_state_retained_audit=False),
            lambda s,e,v:e.pop('WF_TIER3_PARALLEL_STATE_RETAINED_AUDIT'),
            lambda s,e,v:e.update(GOMEMLIMIT='2GiB'),
            lambda s,e,v:e.update(GOMAXPROCS='2'),
            lambda s,e,v:e.update(GOGC='100'),
            lambda s,e,v:e.update(WF_TIER3_CHUNKED_STATE_RETAINED_AUDIT='1'),
            lambda s,e,v:v.pop(0),
            lambda s,e,v:v.pop(),
            lambda s,e,v:v[0].update(Output='MATRIX_AUDIT_MODE mode=parallel_state cutoff=0\n'),
        ):
            observed = copy.deepcopy((state,env,events));mutate(*observed)
            with self.assertRaises(AssertionError):parallel_soak_profile.validate(*observed)

    def test_empty_checkpoint_does_not_displace_required_positive_selection(self):
        state, env, events = self.baseline()
        events.insert(0, {'Output':'MATRIX_AUDIT_MODE mode=parallel_state cutoff=0\n'})
        self.assertEqual(parallel_soak_profile.validate(state, env, events)['selector_markers'],3)

    def test_default_profile_remains_unselected(self):
        self.assertEqual(parallel_soak_profile.validate({}, {}, []), {'selected':False})


if __name__ == '__main__':
    unittest.main()
