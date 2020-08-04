"""Set implementations for Puppetmaster."""

from typing import AbstractSet, Iterable, Iterator

from model import AddressSetMaker, AddressSetMakerFactory


class IdealAddressSetMaker(AddressSetMaker):
    """Wrapper around the built-in set class."""

    __call__ = staticmethod(set)


class IdealAddressSetMakerFactory(AddressSetMakerFactory):
    """Factory for (wrapped) built-in sets."""

    __call__ = staticmethod(IdealAddressSetMaker)


class ApproximateAddressSet(AbstractSet[int]):
    """Bloom filter-like implementation of an integer set."""

    def __init__(self, objects: Iterable[int] = (), /, *, size, n_funcs):
        """Create a new set containing objects."""
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

    def __or__(self, other: AbstractSet) -> "ApproximateAddressSet":
        """Return the union of this set and the other set."""
        if isinstance(other, ApproximateAddressSet):
            out = ApproximateAddressSet(size=self.size, n_funcs=self.n_funcs)
            out.bits = self.bits | other.bits
            return out
        else:
            raise TypeError(
                f"other set must have type {self.__class__.__name__}, not {type(other)}"
            )

    def __and__(self, other: AbstractSet) -> "ApproximateAddressSet":
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
    """Makes ApproximateAddressSet instances."""

    def __init__(self, size: int, n_funcs=1):
        """Create new factory with set size."""
        self.size = size
        self.n_funcs = n_funcs

    def __call__(self, objects: Iterable[int] = ()) -> ApproximateAddressSet:
        """Return new ApproximateAddressSet."""
        return ApproximateAddressSet(objects, size=self.size, n_funcs=self.n_funcs)


class ApproximateAddressSetMakerFactory(AddressSetMakerFactory):
    """Factory for approximate set maker instances with preset arguments."""

    def __init__(self, size: int, n_funcs=1):
        """Create new factory with set size."""
        self.generator = ApproximateAddressSetMaker(size, n_funcs)

    def __call__(self) -> ApproximateAddressSetMaker:
        """Return new ApproximateAddressSet."""
        return self.generator
