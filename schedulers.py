"""Scheduler implementations."""

import heapq
import itertools
import math
from abc import abstractmethod
from typing import Iterable, Optional, Tuple

from api import TransactionScheduler, TransactionSchedulerFactory
from pmtypes import MachineState, Transaction, TransactionSet


class AbstractScheduler(TransactionScheduler):
    """Represents the scheduling unit within Puppetmaster."""

    def __init__(
        self, op_time: int, pool_size: Optional[int], queue_size: Optional[int]
    ):
        """See TransactionSchedulerFactory.__call__."""
        self.op_time = op_time
        self.pool_size = pool_size
        self.queue_size = queue_size

    def run(self, state: MachineState) -> Iterable[MachineState]:
        """See TransactionScheduler.run."""
        state = state.copy()
        # Don't do anything if queue is full.
        if self.queue_size == len(state.scheduled):
            if state.clock < state.cores[0].clock:
                # Scheduler needs to wait until at least one transaction is started.
                state.clock = state.cores[0].clock
            return [state]
        # Fill up pending pool.
        missing = 0
        while self.pool_size is None or len(state.pending) + missing < self.pool_size:
            try:
                if (tr := state.incoming.send(state.obj_set_maker)) is None:
                    missing += 1
                    continue
                else:
                    state.pending.add(tr)
            except StopIteration:
                break
        if not state.pending:
            # We can't accept more transactions into the system.
            if not state.cores:
                raise RuntimeError(
                    "no transactions could be accepted into the empty renaming table, "
                    "the number of hash functions must be increased"
                )
            if state.clock < state.cores[0].clock:
                # Scheduler needs to wait until at least one transaction finishes.
                state.clock = state.cores[0].clock
            return [state]
        # Try scheduling a batch of new transactions.
        ongoing = TransactionSet((core.transaction for core in state.cores))
        for tr in state.scheduled:
            ongoing.add(tr)
        max_count = (
            None if self.queue_size is None else self.queue_size - len(state.scheduled)
        )
        out_states = []
        for scheduled, time in self.schedule(ongoing, state.pending, max_count):
            new_state = state.copy()
            new_state.clock += time
            if scheduled:
                new_state.scheduled.update(scheduled)
                new_state.pending.difference_update(scheduled)
            elif new_state.clock < new_state.cores[0].clock:
                # Scheduler needs to wait until at least one transaction finishes.
                new_state.clock = new_state.cores[0].clock
            out_states.append(new_state)
        return out_states

    @abstractmethod
    def schedule(
        self,
        ongoing: TransactionSet,
        pending: Iterable[Transaction],
        max_count: Optional[int],
    ) -> Iterable[Tuple[TransactionSet, int]]:
        """Schedule one or more transactions."""


class GreedyScheduler(AbstractScheduler):
    """Implementation of a simple scheduler."""

    def schedule(
        self,
        ongoing: TransactionSet,
        pending: Iterable[Transaction],
        max_count: Optional[int],
    ) -> Iterable[Tuple[TransactionSet, int]]:
        """See AbstractScheduler.schedule.

        Iterates through pending transactions once and adds all compatible ones.
        """
        candidates = TransactionSet()
        for tr in pending:
            if ongoing.compatible(tr) and candidates.compatible(tr):
                candidates.add(tr)
                if max_count is not None and len(candidates) == max_count:
                    break
        return [(candidates, self.op_time)]


class GreedySchedulerFactory(TransactionSchedulerFactory):
    """Factory for greedy schedulers."""

    def __call__(
        self, op_time: int = 0, pool_size: int = None, queue_size: int = None
    ) -> GreedyScheduler:
        """See TransactionSchedulerFactory.__call__."""
        return GreedyScheduler(op_time, pool_size, queue_size)

    def __str__(self) -> str:
        """Return human-readable name for the schedulers."""
        return "Greedy scheduler"


class MaximalScheduler(AbstractScheduler):
    """Scheduler that tries to maximize the number of transactions scheduled."""

    n_schedules: int

    def schedule(
        self,
        ongoing: TransactionSet,
        pending: Iterable[Transaction],
        max_count: Optional[int],
    ) -> Iterable[Tuple[TransactionSet, int]]:
        """See AbstractScheduler.schedule."""
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

        def get_result(candidates):
            return (
                candidates
                if max_count is None
                else TransactionSet(itertools.islice(candidates, max_count)),
                self.op_time,
            )

        candidate_sets = all_candidate_sets(TransactionSet(), 0)
        out = heapq.nlargest(self.n_schedules, candidate_sets, key=len)
        return map(get_result, out)


class MaximalSchedulerFactory(TransactionSchedulerFactory):
    """Factory for greedy schedulers."""

    def __init__(self, n_schedules=1):
        """Initialize the factory.

        Arguments:
            n_schedules: the number of possible sets the scheduler returns
        """
        self.n_schedules = n_schedules

    def __call__(
        self, op_time: int = 0, pool_size: int = None, queue_size: int = None
    ) -> MaximalScheduler:
        """See TransactionSchedulerFactory.__call__."""
        sched = MaximalScheduler(op_time, pool_size, queue_size)
        sched.n_schedules = self.n_schedules
        return sched

    def __str__(self) -> str:
        """Return human-readable name for the schedulers."""
        return f"Maximal scheduler ({self.n_schedules} schedule/s)"


class TournamentScheduler(AbstractScheduler):
    """Implementation of a "tournament" scheduler."""

    comparator_limit: Optional[int]

    def schedule(
        self,
        ongoing: TransactionSet,
        pending: Iterable[Transaction],
        max_count: Optional[int],
    ) -> Iterable[Tuple[TransactionSet, int]]:
        """See AbstractScheduler.schedule.

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
            if self.comparator_limit is None:
                rounds += 1
            else:
                rounds += int(math.ceil(len(sets) / (2 * self.comparator_limit)))
            sets = sets[::2]
        if not sets:
            candidates = TransactionSet()
        elif max_count is None:
            candidates = sets[0]
        else:
            candidates = TransactionSet(itertools.islice(sets[0], max_count))
        op_time = self.op_time * max(1, rounds)
        return [(candidates, op_time)]


class TournamentSchedulerFactory(TransactionSchedulerFactory):
    """Factory for greedy schedulers."""

    def __init__(self, comparator_limit: int = None):
        """Initialize the factory.

        Arguments:
            is_pipelined: whether scheduling time depends on the number of merge steps
        """
        if comparator_limit is not None and comparator_limit < 1:
            raise ValueError("comparator limit must be at least 1")
        self.comparator_limit = comparator_limit

    def __call__(
        self, op_time: int = 0, pool_size: int = None, queue_size: int = None
    ) -> TournamentScheduler:
        """See TransactionSchedulerFactory.__call__."""
        sched = TournamentScheduler(op_time, pool_size, queue_size)
        sched.comparator_limit = self.comparator_limit
        return sched

    def __str__(self) -> str:
        """Return human-readable name for the schedulers."""
        comparators = "inf" if self.comparator_limit is None else self.comparator_limit
        return f"Tournament scheduler (comparators={comparators})"
