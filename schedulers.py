"""Scheduler implementations."""

import heapq
import itertools
from abc import abstractmethod
from typing import Iterable, MutableSet, Optional, Tuple

from api import AbstractSetType, TransactionScheduler, TransactionSchedulerFactory
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
        cap = self.pool_size
        while cap is None or len(state.pending) < cap:
            try:
                state.pending.add(state.incoming.send(state.set_maker))
            except StopIteration:
                break
            except ValueError:
                if cap is not None:
                    cap -= 1
                continue
        # Try scheduling a batch of new transactions.
        ongoing = TransactionSet(
            (core.transaction for core in state.cores), obj_set_type=state.set_maker
        )
        for tr in state.scheduled:
            ongoing.add(tr)
        out_states = []
        for scheduled, time in self.schedule(ongoing, state.pending, state.set_maker):
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
        self,
        ongoing: TransactionSet,
        pending: Iterable[Transaction],
        set_type: AbstractSetType[int],
    ) -> Iterable[Tuple[MutableSet[Transaction], int]]:
        """Schedule one or more transactions."""


class GreedyScheduler(AbstractScheduler):
    """Implementation of a simple scheduler."""

    def schedule(
        self,
        ongoing: TransactionSet,
        pending: Iterable[Transaction],
        set_type: AbstractSetType[int],
    ) -> Iterable[Tuple[MutableSet[Transaction], int]]:
        """See AbstractScheduler.schedule.

        Iterates through pending transactions once and adds all compatible ones.
        """
        candidates = TransactionSet(obj_set_type=set_type)
        for tr in pending:
            if ongoing.compatible(tr) and candidates.compatible(tr):
                candidates.add(tr)
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
        set_type: AbstractSetType[int],
    ) -> Iterable[Tuple[MutableSet[Transaction], int]]:
        """See AbstractScheduler.schedule."""
        pending_list = list(pending)

        def all_candidate_sets(prefix, i):
            if i == len(pending_list):
                yield prefix
                return
            yield from all_candidate_sets(prefix, i + 1)
            tr = pending_list[i]
            if ongoing.compatible(tr) and prefix.compatible(tr):
                new_prefix = TransactionSet(prefix, obj_set_type=set_type)
                new_prefix.add(tr)
                yield from all_candidate_sets(new_prefix, i + 1)

        candidate_sets = all_candidate_sets(TransactionSet(obj_set_type=set_type), 0)
        out = heapq.nlargest(self.n_schedules, candidate_sets, key=len)
        return map(lambda x: (x, self.op_time), out)


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
        set_type: AbstractSetType[int],
    ) -> Iterable[Tuple[MutableSet[Transaction], int]]:
        """See AbstractScheduler.schedule.

        Filters out all transactions that conflict with currently running ones, then
        checks the available transactions pairwise against each other repeatedly, until
        a single non-conflicting group remains.
        """
        sets = [
            TransactionSet([tr], obj_set_type=set_type)
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
                (sets[0] if sets else TransactionSet(obj_set_type=set_type)),
                self.op_time * (1 if self.is_pipelined else max(1, rounds)),
            )
        ]


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
