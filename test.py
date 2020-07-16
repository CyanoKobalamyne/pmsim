"""Unit tests for puppetmaster."""
import unittest
from unittest import TestCase

from executors import RandomExecutor
from model import Transaction
from pmtypes import TransactionGenerator
from schedulers import GreedyScheduler
from simulator import Simulator


class TrIter(TransactionGenerator):
    """TransactionGenerator implementation for tests."""

    def __init__(self, transactions):
        """Create a new iterator."""
        self.it = iter(transactions)
        self.length = len(transactions)
        self.index = 0

    def __next__(self):
        """Return next transaction."""
        self.index += 1
        return next(self.it)

    def __bool__(self):
        """Return true if there are still transactions left."""
        return self.index < self.length

    def __len__(self):
        """Return number of transactions left."""
        return self.length - self.length


class TestSimple(TestCase):
    """Simple tests for scheduling 1-2 transactions."""

    def _validate_transactions(self, expected_time, transactions, n_cores=1):
        sched = GreedyScheduler()
        exe = RandomExecutor()
        s = Simulator(TrIter(transactions), sched, exe, n_cores)
        result_time = s.run()[-1].clock
        self.assertEqual(expected_time, result_time)

    def test_01(self):
        """Single transaction without objects."""
        tr = Transaction(set(), set(), 42)
        self._validate_transactions(tr.time, {tr})

    def test_02(self):
        """Single transaction with non-empty read and write sets."""
        tr = Transaction({1, 2}, {3}, 77)
        self._validate_transactions(tr.time, {tr})

    def test_03(self):
        """Two independent transactions running serially."""
        tr1 = Transaction({1}, {2}, 12)
        tr2 = Transaction({3}, {4}, 23)
        expected = tr1.time + tr2.time
        self._validate_transactions(expected, {tr1, tr2})

    def test_04(self):
        """Two independent transactions running concurrently."""
        tr1 = Transaction({1}, {2}, 12)
        tr2 = Transaction({3}, {4}, 23)
        expected = max(tr1.time, tr2.time)
        self._validate_transactions(expected, {tr1, tr2}, n_cores=2)

    def test_05(self):
        """Two transactions reading the same object."""
        tr1 = Transaction({1, 2}, {3}, 31)
        tr2 = Transaction({1, 4}, {5}, 26)
        expected = max(tr1.time, tr2.time)
        self._validate_transactions(expected, {tr1, tr2}, n_cores=2)

    def test_06(self):
        """Two transactions writing the same object."""
        tr1 = Transaction({1, 2}, {3, 4}, 31)
        tr2 = Transaction({5}, {3}, 26)
        expected = tr1.time + tr2.time
        self._validate_transactions(expected, {tr1, tr2}, n_cores=2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
