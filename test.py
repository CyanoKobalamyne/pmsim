"""Unit tests for puppetmaster."""
import unittest
from unittest import TestCase

from executors import RandomExecutor
from model import Transaction
from schedulers import ConstantTimeScheduler
from simulator import Simulator


class TestSimple(TestCase):
    """Simple tests for scheduling 1-2 transactions."""

    def _validate_transactions(self, expected_time, transactions, n_cores=1):
        sched = ConstantTimeScheduler()
        exe = RandomExecutor(n_cores)
        s = Simulator(sched, exe, pool_size=None)
        result_time = s.run(transactions)
        self.assertEqual(expected_time, result_time)

    def test_01(self):
        """Single transaction without objects."""
        tr = Transaction(set(), set(), 42)
        self._validate_transactions(tr.time, {tr})

    def test_02(self):
        """Single transaction with non-empty read and write sets."""
        tr = Transaction({object(), object()}, {object()}, 77)
        self._validate_transactions(tr.time, {tr})

    def test_03(self):
        """Two independent transactions running serially."""
        tr1 = Transaction({object()}, {object()}, 12)
        tr2 = Transaction({object()}, {object()}, 23)
        expected = tr1.time + tr2.time
        self._validate_transactions(expected, {tr1, tr2})

    def test_04(self):
        """Two independent transactions running concurrently."""
        tr1 = Transaction({object()}, {object()}, 12)
        tr2 = Transaction({object()}, {object()}, 23)
        expected = max(tr1.time, tr2.time)
        self._validate_transactions(expected, {tr1, tr2}, n_cores=2)

    def test_05(self):
        """Two transactions reading the same object."""
        obj = object()
        tr1 = Transaction({obj, object()}, {object()}, 31)
        tr2 = Transaction({obj, object()}, {object()}, 26)
        expected = max(tr1.time, tr2.time)
        self._validate_transactions(expected, {tr1, tr2}, n_cores=2)

    def test_06(self):
        """Two transactions writing the same object."""
        obj = object()
        tr1 = Transaction({object(), object()}, {obj, object()}, 31)
        tr2 = Transaction({object()}, {obj}, 26)
        expected = tr1.time + tr2.time
        self._validate_transactions(expected, {tr1, tr2}, n_cores=2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
