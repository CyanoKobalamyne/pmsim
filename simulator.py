"""Main Puppetmaster simulator class."""

import dataclasses
import heapq

from api import ObjSetMaker, TransactionExecutor, TransactionScheduler
from generator import TransactionGenerator
from pmtypes import MachineState


@dataclasses.dataclass(order=True)
class SimulatorState:
    """Internal state of the simulator."""

    time: int
    path: list[MachineState] = dataclasses.field(compare=False)


class Simulator:
    """Simulates executing a set of transactions."""

    def __init__(
        self,
        transactions: TransactionGenerator,
        obj_set_maker: ObjSetMaker,
        scheduler: TransactionScheduler,
        executor: TransactionExecutor,
        core_count: int = 1,
    ) -> None:
        """Create a new simulator.

        Arguments:
            transactions: generator of transactions to execute
            obj_set_maker: object for creating transaction read and write sets
            scheduler: component for scheduling transactions
            executor: component for executing transactions on the processing cores
            core_count: number of processing cores

        """
        self.scheduler = scheduler
        self.executor = executor
        self.start_state = MachineState(
            transactions, obj_set_maker, core_count=core_count
        )

    def run(self, verbose=False) -> list[MachineState]:
        """Simulate execution of a set of transactions on this machine.

        Arguments:
            verbose: print additional debugging info if True

        Returns:
            amount of time (cycles) it took to execute all transactions
        """
        steps = 0
        queue = [SimulatorState(0, [self.start_state])]
        while queue:
            steps += 1

            # Get next state off the queue.
            cur = heapq.heappop(queue)
            state = cur.path[-1]

            if verbose >= 3:
                print(cur.time, steps, len(queue), end="\r")

            if not state:
                # Nothing to do, path ended.
                if verbose >= 2:
                    print(f"{len(cur.path)} states, {steps} steps, {len(queue)} queued")
                return cur.path
            elif len(state.cores) < state.core_count and state.scheduled:
                # Some cores are idle and there are transactions scheduled.
                next_states = self.executor.run(state)
            elif state.cores and state.cores[0].clock <= state.clock:
                # Some transactions have finished executing.
                next_state = state.copy()
                core = heapq.heappop(next_state.cores)  # remove finished transaction.
                next_state.obj_set_maker.free(core.transaction)
                next_state.incoming.reset_overflows()
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
                next_path = cur.path + [next_state]
                heapq.heappush(queue, SimulatorState(time, next_path))

        raise RuntimeError  # We should never get here.
