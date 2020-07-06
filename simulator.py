"""Main Puppetmaster simulator class."""

import heapq
from typing import Iterator

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
            transactions: generator of transactions to execute
            scheduler: component for scheduling transactions
            executor: component for executing transactions on the processing cores
            pool_size: number of tranactions seen by the scheduler simultaneously
                       (all of them if None)

        """
        self.scheduler = scheduler
        self.executor = executor
        self.pool_size = pool_size
        self.start_state = MachineState(transactions, core_count=core_count)

    def run(self) -> int:
        """Simulate execution of a set of transactions on this machine.

        Returns:
            amount of time (cycles) it took to execute all transactions
        """
        queue = [(0, 0, self.start_state)]  # time, step, state
        step = 1
        while queue:
            # Get next state off the queue.
            time, _, state = heapq.heappop(queue)

            if not state:
                return time

            # Run scheduler if conditions are satisfied.
            if not state.scheduled and (
                not state.cores or state.scheduler_clock <= state.cores[0].clock
            ):
                # Fill up pending pool.
                while self.pool_size is None or len(state.pending) < self.pool_size:
                    try:
                        state.pending.add(next(state.incoming))
                    except StopIteration:
                        break
                # Try scheduling a batch of new transactions.
                self.scheduler.run(state)

            # Compute next states for the execution units.
            if len(state.cores) < state.core_count and state.scheduled:
                # Execute a scheduled transaction.
                for next_state in self.executor.run(state):
                    next_time = next_state.cores[0].clock
                    heapq.heappush(queue, (next_time, step, next_state))
                    step += 1
            else:
                # Remove first finished transaction.
                finished_core = heapq.heappop(state.cores)
                # If the scheduler was idle, move its clock forward.
                state.scheduler_clock = max(state.scheduler_clock, finished_core.clock)
                heapq.heappush(queue, (state.scheduler_clock, step, state))
                step += 1

        raise RuntimeError  # We should never get here.
