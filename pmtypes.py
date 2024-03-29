"""Data and container types used in Puppetmaster."""

from __future__ import annotations

import copy
import dataclasses
import itertools
from typing import TYPE_CHECKING, Iterable, Iterator, List, MutableSet, Optional, Set

from api import ObjSet, ObjSetMaker

if TYPE_CHECKING:
    from generator import TransactionGenerator


@dataclasses.dataclass(frozen=True)
class Transaction:
    """An atomic operation in the api."""

    # Make a new id for each transaction. Don't compare other fields (unnecessary).
    id: int = dataclasses.field(init=False, default_factory=itertools.count().__next__)
    read_set: ObjSet = dataclasses.field(compare=False)
    write_set: ObjSet = dataclasses.field(compare=False)
    label: str = dataclasses.field(compare=False)
    time: int = dataclasses.field(compare=False)
    rename_steps: int = dataclasses.field(compare=False)


class TransactionSet(MutableSet[Transaction]):
    """A set of transactions."""

    transactions: Set[Transaction]
    read_set: Optional[ObjSet] = None
    write_set: Optional[ObjSet] = None

    def __init__(self, transactions: Iterable[Transaction] = (), /):
        """Create a new set."""
        self.transactions = set()
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
        if self.read_set is None:
            self.read_set = transaction.read_set.copy()
        else:
            self.read_set |= transaction.read_set
        if self.write_set is None:
            self.write_set = transaction.write_set.copy()
        else:
            self.write_set |= transaction.write_set

    def discard(self, transaction: Transaction) -> None:
        """Remove a transaction from the set.

        Warning: does not update the combined read and write sets.
        """
        self.transactions.discard(transaction)

    def compatible(self, transaction: Transaction) -> bool:
        """Return True if the transaction is compatible with the ones in this set."""
        if not self.transactions or self.read_set is None or self.write_set is None:
            return True
        return (
            not (transaction.read_set & self.write_set)
            and not (transaction.write_set & self.read_set)
            and not (transaction.write_set & self.write_set)
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
    obj_set_maker: ObjSetMaker
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
        new.obj_set_maker = copy.copy(self.obj_set_maker)
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


@dataclasses.dataclass
class SimulationParams:
    """Represents parameters for a single simulation run."""

    clock_period: int
    core_num: int
    pool_size: Optional[int]
    queue_size: Optional[int]
