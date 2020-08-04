"""Main Puppetmaster simulator class."""

import heapq
from typing import List, Tuple

from model import AddressSetMaker, TransactionExecutor, TransactionScheduler
from pmtypes import MachineState, TransactionGenerator


class Simulator:
    """Simulates executing a set of transactions."""

    def __init__(
        self,
        transactions: TransactionGenerator,
        set_maker: AddressSetMaker,
        scheduler: TransactionScheduler,
        executor: TransactionExecutor,
        core_count: int = 1,
    ) -> None:
        """Create a new simulator.

        Arguments:
            transactions: generator of transactions to execute
            set_maker: object for creating transaction read and write sets
            scheduler: component for scheduling transactions
            executor: component for executing transactions on the processing cores
            core_count: number of processing cores

        """
        self.scheduler = scheduler
        self.executor = executor
        self.start_state = MachineState(transactions, set_maker, core_count=core_count)

    def run(self, verbose=False) -> List[MachineState]:
        """Simulate execution of a set of transactions on this machine.

        Arguments:
            verbose: print additional debugging info if True

        Returns:
            amount of time (cycles) it took to execute all transactions
        """
        time = 0
        count = 1
        queue = [(time, count, [self.start_state])]
        while queue:
            # Get next state off the queue.
            time, count, path = heapq.heappop(queue)
            state = path[-1]

            if verbose >= 3:
                print(time, count, len(queue), end="\r")

            if not state:
                # Nothing to do, path ended.
                if verbose >= 2:
                    print(f"States: {len(path)} path, {count} total, {len(queue)} left")
                return path
            elif len(state.cores) < state.core_count and state.scheduled:
                # Some cores are idle and there are transactions scheduled.
                next_states = self.executor.run(state)
            elif state.cores and state.cores[0].clock <= state.clock:
                # Some transactions have finished executing.
                next_state = state.copy()
                core = heapq.heappop(next_state.cores)  # remove finished transaction.
                next_state.set_maker.free(core.transaction)
                next_states = [next_state]
            else:
                # No transactions have finished or nothing is scheduled.
                next_states = self.scheduler.run(state)

            # Push "child" states onto queue.
            for next_state in next_states:
                time = (
                    min(next_state.clock, next_state.cores[0].clock)
                    if next_state.cores
                    else next_state.clock
                )
                count += 1
                next_path = path + [next_state]
                heapq.heappush(queue, (time, count, next_path))

        raise RuntimeError  # We should never get here.
