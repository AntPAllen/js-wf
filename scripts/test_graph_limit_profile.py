import unittest

from graph_limit_profile import audit_profile


class NativeProfileGate(unittest.TestCase):
    def profile(self, operation='CASRoot', **changes):
        row = dict(calls=50, errors=41, total_ns=100, error_kinds={'conflict': 41})
        row.update(changes)
        return {operation: row}

    def test_definite_cas_and_zero_errors(self):
        for operation in ('CASRoot', 'CASBlob'):
            self.assertTrue(audit_profile(self.profile(operation))['accepted'])
        self.assertTrue(audit_profile(self.profile(errors=0, error_kinds={}))['accepted'])

    def test_historical_unclassified_remains_rejected(self):
        p = self.profile()
        del p['CASRoot']['error_kinds']
        result = audit_profile(p)
        self.assertFalse(result['accepted'])
        self.assertEqual(result['unexpected'][0]['errors'], 41)

    def test_uncertain_or_non_cas_errors_rejected(self):
        for kind in ('wrapped_conflict', 'deadline', 'cancelled', 'timeout', 'revoked', 'api', 'other', 'unknown'):
            self.assertFalse(audit_profile(self.profile(error_kinds={kind: 41}))['accepted'])
        self.assertFalse(audit_profile(self.profile(error_kinds={'conflict': 40, 'deadline': 1}))['accepted'])
        self.assertFalse(audit_profile(self.profile('ReadRoot'))['accepted'])

    def test_incomplete_and_impossible_counts_raise(self):
        for changes in (dict(error_kinds={'conflict':40}), dict(error_kinds={'conflict':42}), dict(errors=51), dict(errors=-1), dict(calls=True), dict(total_ns=-1), dict(error_kinds={'conflict':True})):
            with self.assertRaises(ValueError):
                audit_profile(self.profile(**changes))


if __name__ == '__main__':
    unittest.main()
