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

        def cores_clock(state):
            if not state.free_cores:
                return state.busy_cores[0].clock
            elif not state.busy_cores:
                return state.free_cores[0].clock
            return min(state.busy_cores[0].clock, state.free_cores[0].clock)

        queue = [(0, 0, self.start_state)]  # time, step, state
        step = 1
        while queue:
            # Get next state off the queue.
            time, _, state = heapq.heappop(queue)

            if not state:
                return time

            # Run scheduler if conditions are satisfied.
            if not state.scheduled and state.scheduler_clock <= cores_clock(state):
                # Fill up pending pool.
                while self.pool_size is None or len(state.pending) < self.pool_size:
                    try:
                        state.pending.add(next(state.incoming))
                    except StopIteration:
                        break
                # Try scheduling a batch of new transactions.
                self.scheduler.run(state)

            # Compute next states for the execution units.
            if state.free_cores and state.scheduled:
                # If some core were idle while the scheduler was working,
                # move their clocks forward.
                for core in state.free_cores:
                    core.clock = max(core.clock, state.scheduler_clock)
                heapq.heapify(state.free_cores)
                # Execute a scheduled transaction.
                for next_state in self.executor.run(state):
                    next_time = cores_clock(next_state)
                    heapq.heappush(queue, (next_time, step, next_state))
                    step += 1
            else:
                # Remove first finished transaction.
                finished_core = heapq.heappop(state.busy_cores)
                finished_core.transaction = None
                # If the scheduler was idle until the first core freed up, move its
                # clock forward.
                state.scheduler_clock = max(state.scheduler_clock, finished_core.clock)
                # If the free cores are running behind the scheduler, move their clocks
                # forward.
                for core in state.free_cores:
                    core.clock = state.scheduler_clock
                heapq.heapify(state.free_cores)
                finished_core.clock = state.scheduler_clock
                heapq.heappush(state.free_cores, finished_core)
                heapq.heappush(queue, (state.scheduler_clock, step, state))
                step += 1

        raise RuntimeError  # We should never get here.
