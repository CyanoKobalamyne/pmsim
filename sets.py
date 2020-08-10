"""Set implementations for Puppetmaster."""

from __future__ import annotations

import itertools
from typing import AbstractSet, Callable, Iterable, Iterator, MutableMapping, Optional

from api import AddressSetMaker, AddressSetMakerFactory
from pmtypes import Transaction


class IdealAddressSetMaker(AddressSetMaker):
    """Wrapper around the built-in set class."""

    # See https://github.com/python/mypy/issues/3482
    __call__ = staticmethod(set)  # type: ignore


class IdealAddressSetMakerFactory(AddressSetMakerFactory):
    """Factory for (wrapped) built-in sets."""

    # See https://github.com/python/mypy/issues/3482
    __call__ = staticmethod(IdealAddressSetMaker)  # type: ignore

    def __str__(self) -> str:
        """Return human-readable name for the sets."""
        return "Idealized set"


class ApproximateAddressSet(AbstractSet[int]):
    """Bloom filter-like implementation of an integer set."""

    def __init__(self, objects: Iterable[int] = (), /, *, size: int, n_funcs: int):
        """Initialize set to contain objects."""
        self.bits = 0
        self.size = size
        self.n_funcs = n_funcs
        for obj in objects:
            for i in range(n_funcs):
                self.bits |= 1 << ((obj + i) % size)

    def __contains__(self, obj: object) -> bool:
        """Not implemented."""
        raise NotImplementedError

    def __iter__(self) -> Iterator[int]:
        """Not implemented."""
        raise NotImplementedError

    def __len__(self) -> int:
        """Not implemented."""
        raise NotImplementedError

    def __bool__(self) -> bool:
        """Return False if set is empty."""
        return self.bits != 0

    def __or__(self, other: AbstractSet) -> ApproximateAddressSet:
        """Return the union of this set and the other set."""
        if isinstance(other, ApproximateAddressSet):
            out = ApproximateAddressSet(size=self.size, n_funcs=self.n_funcs)
            out.bits = self.bits | other.bits
            return out
        else:
            raise TypeError(
                f"other set must have type {self.__class__.__name__}, not {type(other)}"
            )

    def __and__(self, other: AbstractSet) -> ApproximateAddressSet:
        """Return the intersection of this set and the other set."""
        if isinstance(other, ApproximateAddressSet):
            out = ApproximateAddressSet(size=self.size, n_funcs=self.n_funcs)
            out.bits = self.bits & other.bits
            return out
        else:
            raise TypeError(
                f"other set must have type {self.__class__.__name__}, not {type(other)}"
            )


class ApproximateAddressSetMaker(AddressSetMaker):
    """Makes approximate set instances."""

    def __init__(self, factory: ApproximateAddressSetMakerFactory):
        """Initialize approximate set maker."""
        self.size = factory.size
        self.n_funcs = factory.n_funcs

    def __call__(self, objects: Iterable[int] = ()) -> ApproximateAddressSet:
        """Return new approximate set."""
        return ApproximateAddressSet(objects, size=self.size, n_funcs=self.n_funcs)


class ApproximateAddressSetMakerFactory(AddressSetMakerFactory):
    """Factory for approximate set maker instances with preset arguments."""

    def __init__(self, size: int, n_funcs: int = 1):
        """Initialize factory for approximate address set makers.

        Arguments:
            size: width of bit vector used to represent the set
            n_funcs: number of hash functions used
        """
        self.size = size
        self.n_funcs = n_funcs
        self.generator = ApproximateAddressSetMaker(self)

    def __call__(self) -> ApproximateAddressSetMaker:
        """Return new approximate address set maker."""
        return self.generator

    def __str__(self) -> str:
        """Return human-readable name for the sets."""
        opt = f",  {self.n_funcs} hash functions" if self.n_funcs > 1 else ""
        return f"Approximate set ({self.size} bits{opt})"


class FiniteAddressSet(AbstractSet[int]):
    """Fixed-size set with a global renaming table."""

    def __init__(
        self,
        objects: Iterable[int] = (),
        /,
        *,
        size: int,
        renaming_table: MutableMapping[int, int],
    ):
        """Initialize set to contain objects."""
        self.bits = 0
        self.objs = [-1] * size
        self.size = size
        self.table = renaming_table
        inserted = set()
        for obj in objects:
            if obj in inserted:
                # Ignore duplicates.
                continue
            try:
                index = renaming_table[obj]
                inserted.add(obj)
            except KeyError:
                for iobj in inserted:
                    del renaming_table[iobj]
                raise ValueError("renaming table can't accept this object")
            self.bits |= 1 << index
            self.objs[index] = obj

    def __contains__(self, obj: object) -> bool:
        """Not implemented."""
        raise NotImplementedError

    def __iter__(self) -> Iterator[int]:
        """Yield each object in the set."""
        for obj in self.objs:
            if obj != -1:
                yield obj

    def __len__(self) -> int:
        """Not implemented."""
        raise NotImplementedError

    def __bool__(self) -> bool:
        """Return False if set is empty."""
        return self.bits != 0

    def __or__(self, other: AbstractSet) -> FiniteAddressSet:
        """Return the union of this set and the other set."""
        if isinstance(other, FiniteAddressSet):
            out = FiniteAddressSet(size=self.size, renaming_table=self.table)
            out.bits = self.bits | other.bits
            return out
        else:
            raise TypeError(
                f"other set must have type {self.__class__.__name__}, not {type(other)}"
            )

    def __and__(self, other: AbstractSet) -> FiniteAddressSet:
        """Return the intersection of this set and the other set."""
        if isinstance(other, FiniteAddressSet):
            out = FiniteAddressSet(size=self.size, renaming_table=self.table)
            out.bits = self.bits & other.bits
            return out
        else:
            raise TypeError(
                f"other set must have type {self.__class__.__name__}, not {type(other)}"
            )


class FiniteAddressSetMaker(AddressSetMaker, MutableMapping[int, int]):
    """Makes fixed-size address sets that use a global renaming table."""

    def __init__(self, factory: FiniteAddressSetMakerFactory):
        """Initialize finite set maker."""
        self.size = factory.size
        self.hash_fn = factory.hash_fn
        self.n_hash_funcs = factory.n_hash_funcs
        self.table = [(-1, 0)] * factory.size

    def __call__(self, objects: Iterable[int] = ()) -> FiniteAddressSet:
        """Return new fixed-size set."""
        return FiniteAddressSet(objects, size=self.size, renaming_table=self)

    def free(self, transaction: Transaction) -> None:
        """See AddressSetMaker.free."""
        for obj in itertools.chain(transaction.read_set, transaction.write_set):
            del self[obj]

    def __getitem__(self, obj: int) -> int:
        """Return name for object smaller than the table size."""
        assert obj != -1
        for i in range(self.n_hash_funcs):
            h = self.hash_fn(i, obj)
            prev_obj, count = self.table[h]
            if prev_obj == -1:
                self.table[h] = (obj, 1)
            elif prev_obj == obj:
                self.table[h] = (obj, count + 1)
            else:
                continue
            return h
        raise KeyError("renaming table is full")

    def __delitem__(self, obj: int) -> None:
        """Remove an object from the renaming table."""
        assert obj != -1
        for i in range(self.n_hash_funcs):
            h = self.hash_fn(i, obj)
            prev_obj, count = self.table[h]
            assert prev_obj != -1 and count > 0 or prev_obj == -1 and count == 0
            if prev_obj == obj and count == 1:
                self.table[h] = (-1, 0)
            elif prev_obj == obj and count > 1:
                self.table[h] = (obj, count - 1)
            else:
                continue
            break
        else:
            raise KeyError("object not found")

    def __setitem__(self, obj: int, name: int) -> None:
        """Not implemented."""
        raise NotImplementedError

    def __iter__(self):
        """Not implemented."""
        raise NotImplementedError

    def __len__(self):
        """Not implemented."""
        raise NotImplementedError


class FiniteAddressSetMakerFactory(AddressSetMakerFactory):
    """Factory for fixed-size address set makers with preset arguments."""

    def __init__(
        self,
        size: int,
        hash_fn: Optional[Callable[[int, int], int]] = None,
        n_hash_funcs: Optional[int] = None,
    ):
        """Initialize factory with set size and hash functions."""
        self.size = size
        self.hash_fn = (lambda i, x: (x + i) % size) if hash_fn is None else hash_fn
        self.n_hash_funcs = size if n_hash_funcs is None else n_hash_funcs

    def __call__(self, objects: Iterable[int] = ()) -> FiniteAddressSetMaker:
        """Return new fixed-size set maker."""
        return FiniteAddressSetMaker(self)

    def __str__(self) -> str:
        """Return human-readable name for the sets."""
        return f"Fixed-size set ({self.size} bits, {self.n_hash_funcs} hash functions)"
