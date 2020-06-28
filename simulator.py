"""Main Puppetmaster simulator class."""

import operator

from model import Machine


class Simulator:
    """Simulates executing a set of transactions."""

    def __init__(self, n_cores, pool_size, scheduler):
        """Create a new simulator.

        Arguments:
            n_cores (int): number of execution units (cores) available
            pool_size (int): number of tranactions seen by the scheduler
                             simultaneously (all of them if None)
            scheduler (Scheduler): object used to schedule transactions on
                                   the cores

        """
        self.machine = Machine(n_cores, pool_size)
        self.scheduler = scheduler

    def run(self, transactions):
        """Simulate execution of a set of transactions on this machine.

        Arguments:
            transactions (Iterable[Transaction]): transactions to execute

        Returns:
            int: amount of time (ticks) it took to execute all transactions

        """
        clock_fn = operator.attrgetter("clock")
        scheduler_clock = 0
        tr_iter = iter(transactions)
        pending = set()
        scheduled = set()
        is_tr_left = True
        is_busy = False
        while is_tr_left or pending or scheduled or is_busy:
            free_cores = [
                core for core in self.machine.cores if core.transaction is None
            ]
            transactions = [core.transaction for core in self.machine.cores]
            running = [tr for tr in transactions if tr is not None]
            if not scheduled:
                # Fill up pending pool.
                while (
                    self.machine.pool_size is None
                    or len(pending) < self.machine.pool_size
                ):
                    try:
                        pending.add(next(tr_iter))
                    except StopIteration:
                        is_tr_left = False
                        break
                # Try scheduling a batch of new transactions.
                scheduled, sched_time = self.scheduler.run(pending, running)
                pending -= scheduled
                scheduler_clock += sched_time
            if free_cores and scheduled:
                # Execute scheduled transaction on first idle core.
                core = min(free_cores, key=clock_fn)
                # If the core was idle while the scheduler was working,
                # move its clock forward.
                core.clock = max(scheduler_clock, core.clock)
                # Execute one transaction.
                tr = scheduled.pop()
                core.transaction = tr
                core.clock += tr.time
                is_busy = True
            else:
                # Remove first finished transaction.
                busy_cores = [
                    core for core in self.machine.cores if core.transaction is not None
                ]
                core = min(busy_cores, key=clock_fn)
                finish = core.clock
                core.transaction = None
                for core in free_cores:
                    core.clock = finish
                # If the scheduler was idle until the first core freed up,
                # move its clock forward.
                scheduler_clock = max(finish, scheduler_clock)
                if len(busy_cores) == 1:
                    is_busy = False

        return max(map(clock_fn, self.machine.cores))
