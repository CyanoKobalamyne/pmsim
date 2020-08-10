"""Scheduler implementations."""

import heapq
import itertools
from abc import abstractmethod
from typing import Iterable, Optional, Tuple

from api import AddressSetMaker, TransactionScheduler, TransactionSchedulerFactory
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
                if (tr := state.incoming.send(state.intset_maker)) is None:
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
        ongoing = TransactionSet(
            (core.transaction for core in state.cores), intset_maker=state.intset_maker
        )
        for tr in state.scheduled:
            ongoing.add(tr)
        max_count = (
            None if self.queue_size is None else self.queue_size - len(state.scheduled)
        )
        out_states = []
        for scheduled, time in self.schedule(
            ongoing, state.pending, max_count, state.intset_maker
        ):
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
        intset_maker: AddressSetMaker,
    ) -> Iterable[Tuple[TransactionSet, int]]:
        """Schedule one or more transactions."""


class GreedyScheduler(AbstractScheduler):
    """Implementation of a simple scheduler."""

    def schedule(
        self,
        ongoing: TransactionSet,
        pending: Iterable[Transaction],
        max_count: Optional[int],
        intset_maker: AddressSetMaker,
    ) -> Iterable[Tuple[TransactionSet, int]]:
        """See AbstractScheduler.schedule.

        Iterates through pending transactions once and adds all compatible ones.
        """
        candidates = TransactionSet(intset_maker=intset_maker)
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
        intset_maker: AddressSetMaker,
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
                new_prefix = TransactionSet(prefix, intset_maker=intset_maker)
                new_prefix.add(tr)
                yield from all_candidate_sets(new_prefix, i + 1)

        def get_result(candidates):
            return (
                candidates
                if max_count is None
                else TransactionSet(
                    itertools.islice(candidates, max_count), intset_maker=intset_maker
                ),
                self.op_time,
            )

        candidate_sets = all_candidate_sets(
            TransactionSet(intset_maker=intset_maker), 0
        )
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

    is_pipelined: bool

    def schedule(
        self,
        ongoing: TransactionSet,
        pending: Iterable[Transaction],
        max_count: Optional[int],
        intset_maker: AddressSetMaker,
    ) -> Iterable[Tuple[TransactionSet, int]]:
        """See AbstractScheduler.schedule.

        Filters out all transactions that conflict with currently running ones, then
        checks the available transactions pairwise against each other repeatedly, until
        a single non-conflicting group remains.
        """
        sets = [
            TransactionSet([tr], intset_maker=intset_maker)
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
        if not sets:
            candidates = TransactionSet(intset_maker=intset_maker)
        elif max_count is None:
            candidates = sets[0]
        else:
            candidates = TransactionSet(
                itertools.islice(sets[0], max_count), intset_maker=intset_maker
            )
        if self.is_pipelined:
            op_time = self.op_time
        else:
            op_time = self.op_time * max(1, rounds)
        return [(candidates, op_time)]


class TournamentSchedulerFactory(TransactionSchedulerFactory):
    """Factory for greedy schedulers."""

    def __init__(self, is_pipelined: bool = False):
        """Initialize the factory.

        Arguments:
            is_pipelined: whether scheduling time depends on the number of merge steps
        """
        self.is_pipelined = is_pipelined

    def __call__(
        self, op_time: int = 0, pool_size: int = None, queue_size: int = None
    ) -> TournamentScheduler:
        """See TransactionSchedulerFactory.__call__."""
        sched = TournamentScheduler(op_time, pool_size, queue_size)
        sched.is_pipelined = self.is_pipelined
        return sched

    def __str__(self) -> str:
        """Return human-readable name for the schedulers."""
        opt = " (fully pipelined)" if self.is_pipelined else ""
        return f"Tournament scheduler{opt}"
