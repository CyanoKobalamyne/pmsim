"""Scheduler implementations."""

import itertools
from typing import Iterable

from model import TransactionScheduler
from pmtypes import Transaction, TransactionSet


class ConstantTimeScheduler(TransactionScheduler):
    """Implementation of a simple scheduler."""

    def __init__(self, scheduling_time: int = 0, n_transactions: int = 1):
        """Initialize a new scheduler.

        Arguments:
            scheduling_time (int): constant amount of time the scheduler takes
                                   to choose the next transaction to execute

        """
        self.scheduling_time = scheduling_time
        self.n = n_transactions
        self._clock = 0

    def run(
        self, pending: Iterable[Transaction], ongoing: Iterable[Transaction]
    ) -> TransactionSet:
        """See TransacionScheduler.run."""
        # Filter out candidates compatible with ongoing.
        ongoing = TransactionSet(ongoing)
        candidates = TransactionSet()
        for tr in pending:
            if len(candidates) == self.n:
                break
            if ongoing.compatible(tr) and candidates.compatible(tr):
                candidates.add(tr)
        self._clock += self.scheduling_time
        return candidates

    @property
    def clock(self) -> int:
        """See TimedComponent.clock."""
        return self._clock

    @clock.setter
    def clock(self, value: int) -> None:
        """See TimedComponent.set_clock."""
        self._clock = max(self._clock, value)


class TournamentScheduler(TransactionScheduler):
    """Implementation of a "tournament" scheduler."""

    def __init__(self, cycles_per_merge: int, is_pipelined=False):
        """Initialize a new scheduler.

        Arguments:
            cycles_per_merge: time it takes to perform one "merge" operation in hardware
            is_pipelined: whether scheduling time depends on the number of merge steps
        """
        self.cycles_per_merge = cycles_per_merge
        self.is_pipelined = is_pipelined
        self._clock = 0

    def run(
        self, pending: Iterable[Transaction], ongoing: Iterable[Transaction]
    ) -> TransactionSet:
        """See TransacionScheduler.run.

        Filters out all transactions that conflict with currently running ones, then
        checks the available transactions pairwise against each other repeatedly, until
        a single non-conflicting group remains.
        """
        # Filter out candidates compatible with ongoing.
        ongoing = TransactionSet(ongoing)
        candidates = [TransactionSet([t]) for t in pending if ongoing.compatible(t)]
        rounds = 0
        while len(candidates) > 1:
            for t1, t2 in itertools.zip_longest(candidates[::2], candidates[1::2]):
                if t2 is not None and t1.compatible(t2):
                    t1.add(t2)
            candidates = candidates[::2]
            rounds += 1
        self._clock += self.cycles_per_merge * (1 if self.is_pipelined else rounds)
        return candidates[0] if candidates else TransactionSet()

    @property
    def clock(self) -> int:
        """See TimedComponent.clock."""
        return self._clock

    @clock.setter
    def clock(self, value: int) -> None:
        """See TimedComponent.set_clock."""
        self._clock = max(self._clock, value)
