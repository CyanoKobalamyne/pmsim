class Times:
    MERGE_TRANSACTION = 1
    CHECK_OBJ_CONFLICT = 1


class Machine:
    def __init__(self, n_cores, scheduler):
        # Initialize data.
        self.cores = [[0, None] for _ in range(n_cores)]
        self.scheduler = scheduler()

    def run(self, transactions):
        def clock(core):
            return core[0]
        def transaction(core):
            return core[1]
        pending = set(transactions)
        while pending:
            free_cores = [ix for ix, c in enumerate(self.cores) if transaction(c) is None]
            running = [tr for clk, tr in self.cores if tr is not None]
            tr = self.scheduler.sched_single(pending, running)
            if free_cores and tr:
                # Schedule new transaction on first idle core.
                core = min((self.cores[ix] for ix in free_cores), key=clock)
                core[0] += tr.time  # Advance clock by execution time.
                core[1] = tr
                pending.remove(tr)
            else:
                # Remove first finished transaction.
                core = min(self.cores, key=clock)
                core[1] = None

        return max(self.cores, key=clock)


class Scheduler:
    def sched_single(self, pending, ongoing):
        t = 0
        # Filter out candidates compatible with ongoing.
        tr_set, t1 = TransactionSet.create(pending)
        t += t1
        compatible = set()
        for tr in pending:
            is_comp, t2 = tr_set.compatible(tr)
            t += t2
            if is_comp:
                compatible.add(tr)
        transaction = compatible.pop()
        return transaction, t


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
        t = 0
        self = cls()
        self.transactions = set(transactions)
        for transaction in transactions:
            self.read_set |= transaction.read_set
            self.write_set |= transaction.write_set
            t += Times.MERGE_TRANSACTION
        return self, t

    def compatible(self, transaction):
        t = 0
        for read_obj in transaction.read_set:
            if read_obj in self.write_set:
                return False, t
            t += Times.CHECK_OBJ_CONFLICT
        for write_obj in transaction.write_set:
            if write_obj in self.read_set:
                return False, t
            t += Times.CHECK_OBJ_CONFLICT
            if write_obj in self.write_set:
                return False, t
            t += Times.CHECK_OBJ_CONFLICT
        return True, t
