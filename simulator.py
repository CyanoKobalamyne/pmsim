"""Main Puppetmaster simulator class."""

import heapq
from typing import List

from model import TransactionExecutor, TransactionScheduler
from pmtypes import MachineState, TransactionGenerator


class Simulator:
    """Simulates executing a set of transactions."""

    def __init__(
        self,
        transactions: TransactionGenerator,
        scheduler: TransactionScheduler,
        executor: TransactionExecutor,
        core_count: int = 1,
    ) -> None:
        """Create a new simulator.

        Arguments:
            transactions: generator of transactions to execute
            scheduler: component for scheduling transactions
            executor: component for executing transactions on the processing cores

        """
        self.scheduler = scheduler
        self.executor = executor
        self.start_state = MachineState(transactions, core_count=core_count)

    def run(self, verbose=False) -> List[MachineState]:
        """Simulate execution of a set of transactions on this machine.

        Arguments:
            verbose: print additional debugging info if True

        Returns:
            amount of time (cycles) it took to execute all transactions
        """
        queue = [(0, 0, [self.start_state])]  # time, step, state
        step = 1
        while queue:
            # Get next state off the queue.
            time, _, path = heapq.heappop(queue)
            state = path[-1]

            if not state:
                if verbose:
                    print(f"States: {step} total, {len(queue)} unexplored")
                return path

            # Run scheduler if there are no finished cores.
            if not state.cores or state.clock <= state.cores[0].clock:
                temp_states = self.scheduler.run(state)
            else:
                temp_states = [state]

            for state in temp_states:
                # Compute next states for the execution units.
                if len(state.cores) < state.core_count and state.scheduled:
                    # Execute a scheduled transaction.
                    for next_state in self.executor.run(state):
                        next_time = min(next_state.clock, next_state.cores[0].clock)
                        heapq.heappush(queue, (next_time, step, path + [next_state]))
                        step += 1
                else:
                    # Remove first finished transaction.
                    finished_core = heapq.heappop(state.cores)
                    # If the scheduler was idle, move its clock forward.
                    state.clock = max(state.clock, finished_core.clock)
                    time = (
                        min(state.clock, state.cores[0].clock)
                        if state.cores
                        else state.clock
                    )
                    heapq.heappush(queue, (time, step, path + [state]))
                    step += 1

            if verbose:
                print(time, step, len(queue), end="\r")

        raise RuntimeError  # We should never get here.
