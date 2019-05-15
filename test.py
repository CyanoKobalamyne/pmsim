import unittest
from unittest import TestCase

import puppetmaster as pm


class TestSimple(TestCase):
    def _validate_transactions(self, expected_time, transactions):
        sched = pm.Scheduler()
        m = pm.Machine(1, sched)
        result_time = m.run(transactions)
        self.assertEqual(expected_time, result_time)

    def test_01(self):
        """Single transaction without objects."""
        tr = pm.Transaction(set(), set(), 42)
        self._validate_transactions(tr.time, {tr})

    def test_02(self):
        """Single transaction with non-empty read and write sets."""
        tr = pm.Transaction({object(), object()}, {object()}, 77)
        self._validate_transactions(tr.time, {tr})


if __name__ == "__main__":
    unittest.main()
