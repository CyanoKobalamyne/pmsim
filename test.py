"""Unit tests for puppetmaster."""
import unittest
from unittest import TestCase

import puppetmaster as pm


class TestSimple(TestCase):
    """Simple tests for scheduling 1-2 transactions."""

    def _validate_transactions(self, expected_time, transactions, n_cores=1):
        sched = pm.Scheduler()
        m = pm.Machine(n_cores, sched)
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

    def test_03(self):
        """Two independent transactions running serially."""
        tr1 = pm.Transaction({object()}, {object()}, 12)
        tr2 = pm.Transaction({object()}, {object()}, 23)
        expected = tr1.time + tr2.time
        self._validate_transactions(expected, {tr1, tr2})

    def test_04(self):
        """Two independent transactions running concurrently."""
        tr1 = pm.Transaction({object()}, {object()}, 12)
        tr2 = pm.Transaction({object()}, {object()}, 23)
        expected = max(tr1.time, tr2.time)
        self._validate_transactions(expected, {tr1, tr2}, n_cores=2)

    def test_05(self):
        """Two transactions reading the same object."""
        obj = object()
        tr1 = pm.Transaction({obj, object()}, {object()}, 31)
        tr2 = pm.Transaction({obj, object()}, {object()}, 26)
        expected = max(tr1.time, tr2.time)
        self._validate_transactions(expected, {tr1, tr2}, n_cores=2)

    def test_06(self):
        """Two transactions writing the same object."""
        obj = object()
        tr1 = pm.Transaction({object(), object()}, {obj, object()}, 31)
        tr2 = pm.Transaction({object()}, {obj}, 26)
        expected = tr1.time + tr2.time
        self._validate_transactions(expected, {tr1, tr2}, n_cores=2)


if __name__ == "__main__":
    unittest.main()
