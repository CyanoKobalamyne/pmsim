"""Classes that create transaction generators for Puppetmaster."""

import random
from typing import Generator, List, Mapping, Optional, Sequence, Tuple

from api import ObjSetMaker
from pmtypes import Transaction
from sets import IdealObjSetMaker


class TransactionGenerator(Generator[Optional[Transaction], ObjSetMaker, None]):
    """Yields new transactions based on configuration and available addresses."""

    def __init__(
        self, tr_data: Sequence[Tuple[str, Mapping[str, int]]], addresses: Sequence[int]
    ) -> None:
        """Create new TransactionGenerator.

        Arguments:
            tr_data: configuration for each transaction, which need the following keys:
                "read": size of the read set
                "write": size of the write set
                "time": transaction time
            addresses: addresses available for transactions (assigned sequentially)
        """
        self.tr_data = tr_data
        self.addresses = addresses
        self.tr_index = 0
        self.address_index = 0
        self.overflowed: List[Tuple[int, int]] = []
        self.deferred: List[Tuple[int, int]] = []

    def send(self, obj_set_maker: Optional[ObjSetMaker]) -> Optional[Transaction]:
        """Return next transaction.

        Arguments:
            obj_set_maker: makes sets to be used to store objects in transactions
        """
        if obj_set_maker is None:
            obj_set_maker = IdealObjSetMaker()
        if self.deferred:
            tr_base, address_base = self.deferred.pop(0)
            tr_label, tr_conf = self.tr_data[tr_base]
            read_start = address_base
            read_end = write_start = read_start + tr_conf["reads"]
            write_end = write_start + tr_conf["writes"]
        elif self.tr_index != len(self.tr_data):
            tr_label, tr_conf = self.tr_data[self.tr_index]
            self.tr_index += 1
            read_start = self.address_index
            self.address_index += tr_conf["reads"]
            read_end = write_start = self.address_index
            self.address_index += tr_conf["writes"]
            write_end = self.address_index
        else:
            raise StopIteration
        try:
            read_set = obj_set_maker(self.addresses[read_start:read_end])
            try:
                write_set = obj_set_maker(self.addresses[write_start:write_end])
                tr_time = tr_conf["time"]
                tr_size = tr_conf["reads"] + tr_conf["writes"]
                if obj_set_maker.history is not None:
                    rename_steps = sum(obj_set_maker.history[-tr_size:])
                else:
                    rename_steps = 0
                return Transaction(read_set, write_set, tr_label, tr_time, rename_steps)
            except ValueError:
                # Remove the already inserted objects from the read set.
                obj_set_maker.free_objects(read_set)
                raise
        except ValueError:
            self.overflowed.append((self.tr_index - 1, read_start))
            return None

    def throw(self, exception, value=None, traceback=None):
        """Raise an exception in the generator."""
        self.tr_index = len(self.tr_data)
        return Generator.throw(exception, value, traceback)

    def reset_overflows(self) -> None:
        """Adjust internal state to try overflowing transactions again."""
        self.deferred.extend(self.overflowed)
        self.overflowed = []

    def __bool__(self) -> bool:
        """Return true if there are transactions left."""
        return bool(
            self.tr_index != len(self.tr_data) or self.overflowed or self.deferred
        )

    def __len__(self) -> int:
        """Return number of remaining transactions."""
        return (
            len(self.tr_data)
            - self.tr_index
            + len(self.overflowed)
            + len(self.deferred)
        )

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return (
            f"{self.__class__.__name__}(tr_data={self.tr_data!r}, addresses="
            f"{self.addresses!r}, tr_index={self.tr_index!r}, address_index="
            f"{self.address_index!r})"
        )

    def __str__(self) -> str:
        """Return a user-friendly string representation of this object."""
        return (
            f"{self.__class__.__name__}(Transaction {self.tr_index}/"
            f"{len(self.tr_data)})"
        )


class TransactionGeneratorFactory:
    """Makes new transaction generators based on a parametrized distribution."""

    def __init__(
        self,
        mem_size: int,
        tr_types: Mapping[str, Mapping[str, int]],
        tr_count: int,
        run_count: int = 1,
        zipf_param: int = 0,
    ) -> None:
        """Create a new factory for transactions.

        A random order of transactions is created for each run, with the addresses in
        the read and write sets for each transaction chosen from the range
        [0, mem_size) in accordance with Zipf's law.

        After the __iter__ method has been called run_count times, the factory wraps
        around and returns the same iterators again.

        Arguments:
            mem_size: size of the pool from which objects in the read and
                      write sets are selected
            tr_types: transaction configurations, mappings with the following entries:
                "read": size of the read set
                "write": size of the write set
                "time": transaction time
            tr_count: total number of transactions needed per run
            run_count: number of simulation runs (that should have different input)
            zipf_param: parameter of the Zipf's law distribution
        """
        self.tr_count = tr_count
        self.run_count = run_count
        self.run_index = 0

        # Compute exact transaction and object counts and total time.
        tr_counts = []
        self.obj_count = 0
        self.total_tr_time = 0
        total_weight = sum(tr["weight"] for tr in tr_types.values())
        for tr in tr_types.values():
            cur_tr_count = int(round(tr_count * tr["weight"] / total_weight))
            tr_counts.append(cur_tr_count)
            self.obj_count += cur_tr_count * (tr["reads"] + tr["writes"])
            self.total_tr_time += cur_tr_count * tr["time"]

        # Generate transaction metadata in randomized order.
        one_tr_data: List[Tuple[str, Mapping[str, int]]] = []
        for i, tr_type in enumerate(tr_types.items()):
            one_tr_data.extend(tr_type for _ in range(tr_counts[i]))
        self.tr_data = []
        for _ in range(run_count):
            random.shuffle(one_tr_data)
            self.tr_data.extend(one_tr_data)

        # Generate memory addresses according to distribution.
        addr_count = self.obj_count * run_count
        weights = (
            None
            if zipf_param == 0
            else [1 / (i + 1) ** zipf_param for i in range(mem_size)]
        )
        self.addresses = random.choices(range(mem_size), k=addr_count, weights=weights)

    def __call__(self) -> TransactionGenerator:
        """Return a generator of transactions."""
        if self.run_index == self.run_count:
            self.run_index = 0
        tr_start = self.tr_count * self.run_index
        addr_start = self.obj_count * self.run_index
        self.run_index += 1
        tr_end = self.tr_count * self.run_index
        addr_end = self.obj_count * self.run_index
        tr_data = self.tr_data[tr_start:tr_end]
        addresses = self.addresses[addr_start:addr_end]
        return TransactionGenerator(tr_data, addresses)

    def __len__(self):
        """Return the number of transactions per iterator."""
        return self.tr_count

    @property
    def total_time(self) -> int:
        """Return the time it takes to execute all transactions serially."""
        return self.total_tr_time
