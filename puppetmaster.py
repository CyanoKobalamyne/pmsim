class Machine:
    def __init__(self, n_cores, transactions, scheduler):
        # Initialize data.
        self.clock = 0
        self.pending_transactions = set(transactions)
        self.scheduled_transactions = set()
        self.done_transactions = set()
        self.cores = [[None, 0] for n in range(n_cores)]
        self.scheduler = scheduler

        # Initialize scheduler.
        self.start_scheduler()

    def tick(self):
        # Advance all cores.
        for core in self.cores:
            self.cores[1] += 1
            if core[0] is not None and core[0].time == self.cores[1]:
                # Move current transaction to done.
                t = self.cores[0]
                self.done_transactions.add(t)
                # Execute new transaction.
                try:
                    t = self.scheduled_transactions.pop()
                except KeyError:
                    t = None
                self.cores[0] = t
                self.cores[1] = t.time if t is not None else 0
        # Advance scheduler.
        compatible_transactions = next(self.gen_schedule)
        if compatible_transactions is not None:
            # Schedule transactions.
            self.pending_transactions -= compatible_transactions
            self.scheduled_transactions |= compatible_transactions
            # Restart scheduler.
            self.start_scheduler()

    def start_scheduler(self):
        running_transactions = set(map(lambda c: c[0], self.cores))
        running_transactions |= self.scheduled_transactions
        self.gen_schedule = self.scheduler.run(
            self.pending_transactions, running_transactions)


class Scheduler:
    def __init__(self, n_cores):
        self.n_cores = n_cores

    def run(self, pending_transactions, ongoing_transactions):
        # Filter out candidates compatible with ongoing.
        # TODO: parallelize
        for pending in TransactionSet.create(pending_transactions):
            yield
        # Take 2^n (?), discard and merge pairwise.
        pass


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
            yield
        return self

    def __iter__(self):
        return self.transactions.__iter__()

    def add(self, transaction):
        self.transactions.add(transaction)

    def addall(self, transactions):
        self.transactions |= transactions

    def pop(self):
        if self.transactions:
            return self.transactions.pop()
        else:
            return None

    def compatible(self, transaction_set):
        for read_obj in transaction_set.read_set:
            if read_obj in self.write_set:
                return False
            yield
        for write_obj in transaction_set.write_set:
            if write_obj in self.read_set:
                return False
            yield
            if write_obj in self.write_set:
                return False
            yield
        return True

    def union(self, other):
        pass
