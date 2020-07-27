"""Scheduler implementations."""

import heapq
import itertools
from abc import abstractmethod
from typing import Iterable, MutableSet, Tuple, Type

from model import TransactionScheduler
from pmtypes import MachineState, Transaction, TransactionSet


class AbstractScheduler(TransactionScheduler):
    """Represents the scheduling unit within Puppetmaster."""

    def __init__(
        self,
        op_time: int = 0,
        pool_size: int = None,
        queue_size: int = None,
        set_type: Type[MutableSet[int]] = set,
    ):
        """Create a new scheduler.

        Arguments:
            op_time: number of cycles the scheduler takes to execute a single operation
            pool_size: number of tranactions seen by the scheduler simultaneously
                       (all of them if None)
            queue_size: maximum number of transactions that can be waiting for execution
                        (unlimited if None)
        """
        self.op_time = op_time
        self.pool_size = pool_size
        self.queue_size = queue_size
        self.set_type = set_type

    def run(self, state: MachineState) -> Iterable[MachineState]:
        """Try scheduling a batch of transactions.

        Arguments:
            state: current state of the machine

        Returns:
            transactions ready to be executed concurrently with the currently running
            ones without conflicts
        """
        state = state.copy()
        # Don't do anything if queue is full.
        if self.queue_size == len(state.scheduled):
            if state.clock < state.cores[0].clock:
                # Scheduler needs to wait until at least one transaction is started.
                state.clock = state.cores[0].clock
            return [state]
        # Fill up pending pool.
        while self.pool_size is None or len(state.pending) < self.pool_size:
            try:
                state.pending.add(next(state.incoming))
            except StopIteration:
                break
        # Try scheduling a batch of new transactions.
        ongoing = TransactionSet(
            (core.transaction for core in state.cores), obj_set_type=self.set_type
        )
        for tr in state.scheduled:
            ongoing.add(tr)
        out_states = []
        for scheduled, time in self.schedule(ongoing, state.pending):
            new_state = state.copy()
            new_state.clock += time
            if scheduled:
                count = (
                    self.queue_size - len(new_state.scheduled)
                    if self.queue_size is not None
                    else None
                )
                scheduled = set(itertools.islice(scheduled, count))
                new_state.scheduled |= scheduled
                new_state.pending -= scheduled
            elif new_state.clock < new_state.cores[0].clock:
                # Scheduler needs to wait until at least one transaction finishes.
                new_state.clock = new_state.cores[0].clock
            out_states.append(new_state)
        return out_states

    @abstractmethod
    def schedule(
        self, ongoing: TransactionSet, pending: Iterable[Transaction]
    ) -> Iterable[Tuple[MutableSet[Transaction], int]]:
        """Schedule one or more transactions."""


class GreedyScheduler(AbstractScheduler):
    """Implementation of a simple scheduler."""

    def schedule(
        self, ongoing: TransactionSet, pending: Iterable[Transaction]
    ) -> Iterable[Tuple[MutableSet[Transaction], int]]:
        """See TransacionScheduler.schedule.

        Iterates through pending transactions once and adds all compatible ones.
        """
        candidates = TransactionSet(obj_set_type=self.set_type)
        for tr in pending:
            if ongoing.compatible(tr) and candidates.compatible(tr):
                candidates.add(tr)
        return [(candidates, self.op_time)]

    @property
    def name(self):
        """See TransacionScheduler.name."""
        return "Greedy scheduler"


class MaximalScheduler(AbstractScheduler):
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
                new_prefix = TransactionSet(prefix, obj_set_type=self.set_type)
                new_prefix.add(tr)
                yield from all_candidate_sets(new_prefix, i + 1)

        sets = all_candidate_sets(TransactionSet(obj_set_type=self.set_type), 0)
        out = heapq.nlargest(self.n_schedules, sets, key=len)
        return map(lambda x: (x, self.op_time), out)

    @property
    def name(self):
        """See TransacionScheduler.name."""
        return "Maximal scheduler"


class TournamentScheduler(AbstractScheduler):
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
        sets = [
            TransactionSet([tr], obj_set_type=self.set_type)
            for tr in pending
            if ongoing.compatible(tr)
        ]
        rounds = 0
        while len(sets) > 1:
            for t1, t2 in itertools.zip_longest(sets[::2], sets[1::2]):
                if t2 is not None and t1.compatible(t2):
                    t1 |= t2
            sets = sets[::2]
            rounds += 1
        return [
            (
                (sets[0] if sets else TransactionSet(obj_set_type=self.set_type)),
                self.op_time * (1 if self.is_pipelined else max(1, rounds)),
            )
        ]

    @property
    def name(self):
        """See TransacionScheduler.name."""
        return (
            f"Tournament scheduler{' (fully pipelined)' if self.is_pipelined else ''}"
        )
