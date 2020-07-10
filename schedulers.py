"""Scheduler implementations."""

import heapq
import itertools
from typing import Iterable, MutableSet, Tuple

from model import TransactionScheduler
from pmtypes import Transaction, TransactionSet


class GreedyScheduler(TransactionScheduler):
    """Implementation of a simple scheduler."""

    def schedule(
        self, ongoing: TransactionSet, pending: Iterable[Transaction]
    ) -> Iterable[Tuple[MutableSet[Transaction], int]]:
        """See TransacionScheduler.schedule.

        Iterates through pending transactions once and adds all compatible ones.
        """
        candidates = TransactionSet()
        for tr in pending:
            if ongoing.compatible(tr) and candidates.compatible(tr):
                candidates.add(tr)
        return [(candidates, self.op_time)]


class MaximalScheduler(TransactionScheduler):
    """Scheduler that tries to maximize the number of transactions scheduled."""

    def __init__(self, *args, n_schedules=1):
        """Initialize a new scheduler.

        Arguments:
            n_schedules: the number of possible sets to return
        """
        super().__init__(*args)
        self.n_schedules = n_schedules

    def schedule(
        self, ongoing: TransactionSet, pending: Iterable[Transaction]
    ) -> Iterable[Tuple[MutableSet[Transaction], int]]:
        """See TransacionScheduler.schedule."""
        pending_list = list(pending)

        def all_candidate_sets(prefix, i):
            if i == len(pending_list):
                yield prefix
                return
            yield from all_candidate_sets(prefix, i + 1)
            tr = pending_list[i]
            if ongoing.compatible(tr) and prefix.compatible(tr):
                new_prefix = TransactionSet(prefix)
                new_prefix.add(tr)
                yield from all_candidate_sets(new_prefix, i + 1)

        sets = all_candidate_sets(TransactionSet(), 0)
        out = heapq.nlargest(self.n_schedules, sets, key=len)
        return map(lambda x: (x, self.op_time), out)


class TournamentScheduler(TransactionScheduler):
    """Implementation of a "tournament" scheduler."""

    def __init__(self, *args, is_pipelined=False):
        """Initialize a new scheduler.

        Arguments:
            is_pipelined: whether scheduling time depends on the number of merge steps
        """
        super().__init__(*args)
        self.is_pipelined = is_pipelined

    def schedule(
        self, ongoing: TransactionSet, pending: Iterable[Transaction]
    ) -> Iterable[Tuple[MutableSet[Transaction], int]]:
        """See TransacionScheduler.schedule.

        Filters out all transactions that conflict with currently running ones, then
        checks the available transactions pairwise against each other repeatedly, until
        a single non-conflicting group remains.
        """
        sets = [TransactionSet([tr]) for tr in pending if ongoing.compatible(tr)]
        rounds = 0
        while len(sets) > 1:
            for t1, t2 in itertools.zip_longest(sets[::2], sets[1::2]):
                if t2 is not None and t1.compatible(t2):
                    t1 |= t2
            sets = sets[::2]
            rounds += 1
        return [
            (
                (sets[0] if sets else TransactionSet()),
                self.op_time * (1 if self.is_pipelined else max(1, rounds)),
            )
        ]
