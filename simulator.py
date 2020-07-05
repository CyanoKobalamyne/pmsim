"""Main Puppetmaster simulator class."""

import heapq
import operator
from typing import Iterator, List, Tuple

from model import TransactionExecutor, TransactionScheduler
from pmtypes import MachineState, Transaction


class Simulator:
    """Simulates executing a set of transactions."""

    def __init__(
        self,
        transactions: Iterator[Transaction],
        scheduler: TransactionScheduler,
        executor: TransactionExecutor,
        core_count: int = 1,
        pool_size: int = None,
    ) -> None:
        """Create a new simulator.

        Arguments:
            transactions (Iterable[Transaction]): transactions to execute
            scheduler: component for scheduling transactions
            executor: component for executing transactions on the processing cores
            pool_size (int): number of tranactions seen by the scheduler
                             simultaneously (all of them if None)

        """
        self.scheduler = scheduler
        self.executor = executor
        self.poolsize = pool_size
        self.start_state = MachineState(transactions, core_count=core_count)

    def run(self) -> int:
        """Simulate execution of a set of transactions on this machine.

        Returns:
            int: amount of time (ticks) it took to execute all transactions

        """
        queue: List[Tuple[int, MachineState]] = []
        heapq.heappush(queue, (0, self.start_state))
        while queue:
            # Get next state off the queue.
            time, state = heapq.heappop(queue)

            if not state:
                return time

            free_cores = [core for core in state.cores if core.transaction is None]

            # Run scheduler if conditions are satisfied.
            if not state.scheduled and state.scheduler_clock <= min(
                core.clock for core in state.cores
            ):
                # Fill up pending pool.
                self._fill_pool(state)
                # Try scheduling a batch of new transactions.
                self.scheduler.run(state)

            # Compute next states for the execution units.
            if free_cores and state.scheduled:
                # If some core were idle while the scheduler was working,
                # move their clocks forward.
                for core in free_cores:
                    core.clock = max(core.clock, state.scheduler_clock)
                # Execute a scheduled transaction.
                for next_state in self.executor.run(state):
                    next_time = min(c.clock for c in next_state.cores)
                    heapq.heappush(queue, (next_time, next_state))
            else:
                # Remove first finished transaction.
                busy_cores = [
                    core for core in state.cores if core.transaction is not None
                ]
                finished_core = min(busy_cores, key=operator.attrgetter("clock"))
                finish = finished_core.clock
                finished_core.transaction = None
                if len(busy_cores) == 1:
                    state.is_busy = False
                # If the scheduler was idle until the first core freed up, move its
                # clock forward.
                state.scheduler_clock = max(state.scheduler_clock, finish)
                # If the free cores are running behind the scheduler, move their clocks
                # forward.
                for core in free_cores:
                    core.clock = state.scheduler_clock
                finished_core.clock = state.scheduler_clock
                heapq.heappush(queue, (state.scheduler_clock, state))

        raise RuntimeError  # We should never get here.

    def _fill_pool(self, state: MachineState) -> None:
        """Fill up the scheduling pool."""
        while self.poolsize is None or len(state.pending) < self.poolsize:
            try:
                state.pending.add(next(state.incoming))
            except StopIteration:
                break
