"""Data and container types used in Puppetmaster."""

from __future__ import annotations

import copy
import dataclasses
from typing import Iterable, Iterator, List, MutableSet, Set


class Transaction:
    """An atomic operation in the model."""

    id: int
    _next_id = 0

    def __new__(cls, *args, **kwargs):
        """Return a new instance of Transaction."""
        instance = super().__new__(cls)
        instance.id = Transaction._next_id
        Transaction._next_id += 1
        return instance

    def __init__(
        self, read_set: Iterable[int], write_set: Iterable[int], time: int
    ) -> None:
        """Create a transaction.

        Attributes:
            read_set: the set of objects that this transaction needs to read
            write_set: the set of objects that this transaction needs to write
                       and possibly also read
            time: the amount of time units it takes to execute this transaction

        """
        self.read_set = set(read_set)
        self.write_set = set(write_set)
        self.time = time

    def __hash__(self) -> int:
        """Return a hash value for this transaction."""
        return self.id  # pylint: disable=no-member

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

    def __init__(self, transactions: Iterable[Transaction] = ()):
        """Create a new set."""
        self.transactions: Set[Transaction] = set()
        self.read_set: Set[int] = set()
        self.write_set: Set[int] = set()
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
        self.read_set |= transaction.read_set
        self.write_set |= transaction.write_set

    def discard(self, transaction: Transaction) -> None:
        """Remove a transaction from the set.

        Warning: does not update the combined read and write sets.
        """
        self.transactions.discard(transaction)

    def compatible(self, transaction: Transaction) -> bool:
        """Return whether the given transaction is compatible with this set."""
        for read_obj in transaction.read_set:
            if read_obj in self.write_set:
                return False
        for write_obj in transaction.write_set:
            if write_obj in self.read_set or write_obj in self.write_set:
                return False
        return True


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

    incoming: Iterator[Transaction]
    pending: MutableSet[Transaction] = dataclasses.field(default_factory=set)
    scheduled: MutableSet[Transaction] = dataclasses.field(default_factory=set)
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
        new.incoming = copy.copy(self.incoming)
        new.pending = set(self.pending)
        new.scheduled = set(self.scheduled)
        new.cores = list(Core(c.clock, c.transaction) for c in self.cores)
        return new
