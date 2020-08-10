"""Data and container types used in Puppetmaster."""

from __future__ import annotations

import copy
import dataclasses
import itertools
from typing import (
    AbstractSet,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableSet,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from api import AddressSetMaker


class UniqIdMaker:
    """Creates unique ids for class instances for the lifetime of the program."""

    counters: Dict[type, Iterator[int]] = {}

    @classmethod
    def next_id(cls, type_: type):
        """Return the next id corresponding to an instance of the given type."""
        if type_ not in cls.counters:
            cls.counters[type_] = itertools.count()
        return next(cls.counters[type_])


class Transaction:
    """An atomic operation in the api."""

    def __init__(
        self, read_set: AbstractSet[int], write_set: AbstractSet[int], time: int,
    ) -> None:
        """Create a transaction.

        Attributes:
            read_set: the set of objects that this transaction needs to read
            write_set: the set of objects that this transaction needs to write
                       and possibly also read
            time: the amount of time units it takes to execute this transaction
            intset_maker: makes sets used for keeping track of read and written objects
        """
        self.read_set = read_set
        self.write_set = write_set
        self.time = time
        self.id = UniqIdMaker.next_id(Transaction)

    def __hash__(self) -> int:
        """Return a hash value for this transaction."""
        return self.id

    def __eq__(self, other: object) -> bool:
        """Return True if the two transactions are the same."""
        if isinstance(other, Transaction):
            return self.id == other.id
        return False

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return (
            f"{self.__class__.__name__}(id={self.id!r}, read_set="
            f"{self.read_set!r}, write_set={self.write_set!r}, time="
            f"{self.time!r})"
        )

    def __str__(self) -> str:
        """Return a user-friendly string representation of this object."""
        return f"{self.__class__.__name__}(id={self.id}, time={self.time})"


class TransactionSet(MutableSet[Transaction]):
    """A set of transactions."""

    def __init__(self, transactions: Iterable[Transaction] = (), /):
        """Create a new set."""
        self.transactions: Set[Transaction] = set()
        for t in transactions:
            self.add(t)

    def __contains__(self, transaction: object) -> bool:
        """Return if the given transaction is in the set."""
        return transaction in self.transactions

    def __iter__(self) -> Iterator[Transaction]:
        """Yield each transaction in the set."""
        yield from self.transactions

    def __len__(self) -> int:
        """Return the number of transactions in the set."""
        return len(self.transactions)

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return f"{self.__class__.__name__}({self.transactions})"

    def __str__(self) -> str:
        """Return a user-friendly string representation of this object."""
        return repr(self)

    def add(self, transaction: Transaction) -> None:
        """Add a new transaction to the set."""
        self.transactions.add(transaction)
        if not hasattr(self, "read_set"):
            self.read_set = transaction.read_set
        else:
            self.read_set = transaction.read_set | self.read_set
        if not hasattr(self, "write_set"):
            self.write_set = transaction.write_set
        else:
            self.write_set = transaction.write_set | self.write_set

    def discard(self, transaction: Transaction) -> None:
        """Remove a transaction from the set.

        Warning: does not update the combined read and write sets.
        """
        self.transactions.discard(transaction)

    def compatible(self, transaction: Transaction) -> bool:
        """Return True if the transaction is compatible with the ones in this set."""
        if not self.transactions:
            return True
        return (
            not (transaction.read_set & self.write_set)
            and not (transaction.write_set & self.read_set)
            and not (transaction.write_set & self.write_set)
        )


class TransactionGenerator(Generator[Optional[Transaction], AddressSetMaker, None]):
    """Yields new transactions based on configuration and available addresses."""

    def __init__(
        self, tr_data: Iterable[Mapping[str, int]], addresses: Sequence[int]
    ) -> None:
        """Create new TransactionGenerator.

        Arguments:
            tr_data: configuration for each transaction, which need the following keys:
                "read": size of the read set
                "write": size of the write set
                "time": transaction time
            addresses: addresses available for transactions (assigned sequentially)
        """
        self.tr_data = list(tr_data)
        self.addresses = addresses
        self.tr_index = 0
        self.address_index = 0
        self.overflowed: List[Tuple[int, int]] = []
        self.deferred: List[Tuple[int, int]] = []

    def send(self, intset_maker: AddressSetMaker) -> Optional[Transaction]:
        """Return next transaction.

        Arguments:
            intset_maker: makes sets to be used to store objects in transactions
        """
        if self.deferred:
            tr_base, address_base = self.deferred.pop(0)
            tr_conf = self.tr_data[tr_base]
            read_start = address_base
            read_end = write_start = read_start + tr_conf["reads"]
            write_end = write_start + tr_conf["writes"]
        elif self.tr_index != len(self.tr_data):
            tr_conf = self.tr_data[self.tr_index]
            self.tr_index += 1
            read_start = self.address_index
            self.address_index += tr_conf["reads"]
            read_end = write_start = self.address_index
            self.address_index += tr_conf["writes"]
            write_end = self.address_index
        else:
            raise StopIteration
        try:
            read_set = intset_maker(self.addresses[read_start:read_end])
            try:
                write_set = intset_maker(self.addresses[write_start:write_end])
                return Transaction(read_set, write_set, tr_conf["time"])
            except ValueError:
                # Remove the already inserted objects from the read set.
                intset_maker.free(Transaction(read_set, set(), 0))
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


@dataclasses.dataclass(order=True)
class Core:
    """Component executing a single transaction.

    Attributes:
        clock (int): time elapsed since the start of the machine in "ticks"
        transaction (Transaction): the transaction being executed or None if
                                   the core is idle

    """

    clock: int
    transaction: Transaction = dataclasses.field(compare=False)


@dataclasses.dataclass
class MachineState:
    """Represents the full state of the machine. Useful for state space search."""

    incoming: TransactionGenerator
    intset_maker: AddressSetMaker
    pending: Set[Transaction] = dataclasses.field(default_factory=set)
    scheduled: Set[Transaction] = dataclasses.field(default_factory=set)
    core_count: int = 1
    cores: List[Core] = dataclasses.field(default_factory=list)
    clock: int = 0  # global clock, same as clock of the scheduler.

    def __bool__(self):
        """Return true if this is not an end state."""
        return bool(self.incoming or self.pending or self.scheduled or self.cores)

    def copy(self) -> MachineState:
        """Make a 1-deep copy of this object.

        Collection fields are recreated, but the contained object will be the same.
        """
        new = copy.copy(self)
        new.intset_maker = copy.copy(self.intset_maker)
        new.incoming = copy.copy(self.incoming)
        new.pending = set(self.pending)
        new.scheduled = set(self.scheduled)
        new.cores = list(Core(c.clock, c.transaction) for c in self.cores)
        return new

    def __str__(self):
        """Return user-friendly string representation of this object."""
        return (
            f"<{self.__class__.__name__} at {self.clock}: {len(self.incoming)} "
            f"incoming, {len(self.pending)} pending, {len(self.scheduled)} scheduled, "
            f"{len(self.cores)} running>"
        )
