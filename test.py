"""Unit tests for puppetmaster."""
import unittest
from collections.abc import Sized
from typing import Generator, Iterable
from unittest import TestCase

from api import AddressSetMaker
from executors import RandomExecutor
from pmtypes import Transaction
from schedulers import GreedyScheduler
from sets import IdealAddressSetMaker
from simulator import Simulator


class TestTransactionGenerator(Generator[Transaction, AddressSetMaker, None], Sized):
    """TransactionGenerator implementation for tests."""

    def __init__(self, transactions: Iterable[Transaction]):
        """Create a new iterator."""
        self.transactions = list(transactions)
        self.index = 0

    def send(self, value: AddressSetMaker) -> Transaction:
        """Return next transaction."""
        if self.index == len(self.transactions):
            raise StopIteration
        self.index += 1
        return self.transactions[self.index - 1]

    def throw(self, exc, val, tb):
        """Raise an exception in the generator."""
        self.index = len(self.transactions)
        raise exc

    def __bool__(self):
        """Return true if there are still transactions left."""
        return self.index < len(self.transactions)

    def __len__(self):
        """Return number of transactions left."""
        return len(self.transactions) - self.index


class TestSimple(TestCase):
    """Simple tests for scheduling 1-2 transactions."""

    def _validate_transactions(self, expected_time, transactions, n_cores=1):
        sched = GreedyScheduler()
        exe = RandomExecutor()
        s = Simulator(
            TestTransactionGenerator(transactions),
            IdealAddressSetMaker(),
            sched,
            exe,
            n_cores,
        )
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
