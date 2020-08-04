"""Set implementations for Puppetmaster."""

from __future__ import annotations

import itertools
from typing import AbstractSet, Callable, Iterable, Iterator, List, Optional, Tuple

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
        """Return human-readable name for the factory (set)."""
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
        """Return human-readable name for the factory (set)."""
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
        hash_fn: Callable[[int, int], int],
        renaming_table: List[Tuple[int, int]],
    ):
        """Initialize set to contain objects."""
        self.bits = 0
        self.size = size
        self.hash_fn = hash_fn
        self.table = renaming_table
        for obj in objects:
            self.bits |= 1 << self.__get_name(obj)

    def __get_name(self, obj: int) -> int:
        for i in range(self.size):
            h = self.hash_fn(i, obj)
            prev_obj, count = self.table[h]
            if prev_obj == -1:
                self.table[h] = (obj, 1)
            elif prev_obj == obj:
                self.table[h] = (obj, count + 1)
            else:
                continue
            return h
        raise ValueError("renaming table is full")

    def __contains__(self, obj: object) -> bool:
        """Not implemented."""
        raise NotImplementedError

    def __iter__(self) -> Iterator[int]:
        """Yield each object in the set."""
        for obj, count in self.table:
            assert obj == -1 and count == 0 or obj != -1 and count > 0
            for _ in range(count):
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
            out = FiniteAddressSet(
                size=self.size, hash_fn=self.hash_fn, renaming_table=self.table
            )
            out.bits = self.bits | other.bits
            return out
        else:
            raise TypeError(
                f"other set must have type {self.__class__.__name__}, not {type(other)}"
            )

    def __and__(self, other: AbstractSet) -> FiniteAddressSet:
        """Return the intersection of this set and the other set."""
        if isinstance(other, FiniteAddressSet):
            out = FiniteAddressSet(
                size=self.size, hash_fn=self.hash_fn, renaming_table=self.table
            )
            out.bits = self.bits & other.bits
            return out
        else:
            raise TypeError(
                f"other set must have type {self.__class__.__name__}, not {type(other)}"
            )


class FiniteAddressSetMaker(AddressSetMaker):
    """Makes fixed-size address sets that use a global renaming table."""

    def __init__(self, factory: FiniteAddressSetMakerFactory):
        """Initialize finite set maker."""
        self.size = factory.size
        self.hash_fn = factory.hash_fn
        self.table = [(-1, 0)] * factory.size

    def __call__(self, objects: Iterable[int] = ()) -> FiniteAddressSet:
        """Return new fixed-size set."""
        return FiniteAddressSet(
            objects, size=self.size, hash_fn=self.hash_fn, renaming_table=self.table
        )

    def free(self, transaction: Transaction) -> None:
        """See AddressSetMaker.free."""
        for obj in itertools.chain(transaction.read_set, transaction.write_set):
            for i in range(self.size):
                h = self.hash_fn(i, obj)
                prev_obj, count = self.table[h]
                if prev_obj == obj and count == 1:
                    self.table[h] = (-1, 0)
                elif prev_obj == obj and count > 1:
                    self.table[h] = (obj, count - 1)
                else:
                    continue
                break
            else:
                raise KeyError("object not found")


class FiniteAddressSetMakerFactory(AddressSetMakerFactory):
    """Factory for fixed-size address set makers with preset arguments."""

    def __init__(self, size: int, hash_fn: Optional[Callable[[int, int], int]] = None):
        """Initialize factory with set size and hash functions."""
        self.size = size
        self.hash_fn = lambda i, x: (x + i) % size if hash_fn is None else hash_fn

    def __call__(self, objects: Iterable[int] = ()) -> FiniteAddressSetMaker:
        """Return new fixed-size set maker."""
        return FiniteAddressSetMaker(self)

    def __str__(self) -> str:
        """Return human-readable name for the factory (set)."""
        return f"Fixed-size set using global renaming table ({self.size} bits)"
