import operator


SCHEDULING_TIME = 0


class Core:
    """Component of `Machine` executing a single transaction.

    Attributes:
        clock (int): time elapsed since the start of the machine in "ticks"
        transaction (Transaction): the transaction being executed or None if
                                   the core is idle

    """

    def __init__(self, clock_start=0, transaction=None):
        """Create a new core.

        Arguments:
            clock_start (int): initial value for the clock
            transaction: initial transaction being executed

        """
        self.clock = clock_start
        self.transaction = transaction


class Machine:
    """Device capable of executing transactions in parallel."""

    def __init__(self, n_cores, scheduler):
        """Create a new machine.

        Arguments:
            n_cores (int): number of execution units (cores) available
            scheduler (Scheduler): object used to schedule transactions on
                                   the cores

        """
        self.cores = [Core() for _ in range(n_cores)]
        self.scheduler = scheduler

    def run(self, transactions):
        """Simulate execution of a set of transactions on this machine.

        Arguments:
            transactions (Iterable[Transaction]): transactions to execute

        Returns:
            int: amount of time (ticks) it took to execute all transactions

        """
        clock_fn = operator.attrgetter('clock')
        scheduler_clock = 0
        tr = None
        pending = set(transactions)
        while pending:
            free_cores = [core for core in self.cores
                          if core.transaction is None]
            transactions = [core.transaction for core in self.cores]
            running = [tr for tr in transactions if tr is not None]
            if not tr:
                # Try scheduling a new transaction.
                tr = self.scheduler.sched_single(pending, running)
                scheduler_clock += SCHEDULING_TIME
            if free_cores and tr:
                # Execute scheduled transaction on first idle core.
                core = min(free_cores, key=clock_fn)
                # If the core was idle while the scheduler was working,
                # move its clock forward.
                core.clock = max(scheduler_clock, core.clock)
                core.clock += tr.time  # Advance clock by execution time.
                core.transaction = tr
                pending.remove(tr)
                tr = None
            else:
                # Remove first finished transaction.
                busy_cores = [core for core in self.cores
                              if core.transaction is not None]
                core = min(busy_cores, key=clock_fn)
                finish = core.clock
                core.transaction = None
                for core in free_cores:
                    core.clock = finish
                # If the scheduler was idle until the first core freed up,
                # move its clock forward.
                scheduler_clock = max(finish, scheduler_clock)

        return max(map(clock_fn, self.cores))


class Scheduler:
    def sched_single(self, pending, ongoing):
        # Filter out candidates compatible with ongoing.
        tr_set = TransactionSet.create(ongoing)
        compatible = set()
        for tr in pending:
            is_comp = tr_set.compatible(tr)
            if is_comp:
                compatible.add(tr)
        try:
            transaction = compatible.pop()
        except KeyError:
            # No compatible transaction.
            transaction = None
        return transaction


class Transaction:
    def __init__(self, read_set, write_set, time):
        self.read_set = read_set
        self.write_set = write_set
        self.time = time

    def __hash__(self):
        return id(self)


class TransactionSet:
    def __init__(self):
        self.read_set = set()
        self.write_set = set()

    @classmethod
    def create(cls, transactions):
        self = cls()
        self.transactions = set(transactions)
        for transaction in transactions:
            self.read_set |= transaction.read_set
            self.write_set |= transaction.write_set
        return self

    def compatible(self, transaction):
        for read_obj in transaction.read_set:
            if read_obj in self.write_set:
                return False
        for write_obj in transaction.write_set:
            if write_obj in self.read_set:
                return False
            if write_obj in self.write_set:
                return False
        return True
