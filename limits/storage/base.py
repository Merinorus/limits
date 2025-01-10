from __future__ import annotations

import functools
import threading
from abc import ABC, abstractmethod
from typing import Any, cast

from limits import errors
from limits.storage.registry import StorageRegistry
from limits.typing import (
    Callable,
    List,
    Optional,
    P,
    R,
    Tuple,
    Type,
    Union,
)
from limits.util import LazyDependency


def _wrap_errors(storage: Storage, fn: Callable[P, R]) -> Callable[P, R]:
    @functools.wraps(fn)
    def inner(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return fn(*args, **kwargs)
        except storage.base_exceptions as exc:
            if storage.wrap_exceptions:
                raise errors.StorageError(exc) from exc
            raise

    return inner


class Storage(LazyDependency, metaclass=StorageRegistry):
    """
    Base class to extend when implementing a storage backend.
    """

    STORAGE_SCHEME: Optional[List[str]]
    """The storage schemes to register against this implementation"""

    def __new__(cls, *args: Any, **kwargs: Any) -> Storage:  # type: ignore[misc]
        inst = super().__new__(cls)

        for method in {
            "incr",
            "get",
            "get_expiry",
            "check",
            "reset",
            "clear",
        }:
            setattr(inst, method, _wrap_errors(inst, getattr(inst, method)))

        return inst

    def __init__(
        self,
        uri: Optional[str] = None,
        wrap_exceptions: bool = False,
        **options: Union[float, str, bool],
    ):
        """
        :param wrap_exceptions: Whether to wrap storage exceptions in
         :exc:`limits.errors.StorageError` before raising it.
        """

        self.lock = threading.RLock()
        super().__init__()
        self.wrap_exceptions = wrap_exceptions

    @property
    @abstractmethod
    def base_exceptions(self) -> Union[Type[Exception], Tuple[Type[Exception], ...]]:
        raise NotImplementedError

    @abstractmethod
    def incr(
        self, key: str, expiry: int, elastic_expiry: bool = False, amount: int = 1
    ) -> int:
        """
        increments the counter for a given rate limit key

        :param key: the key to increment
        :param expiry: amount in seconds for the key to expire in
        :param elastic_expiry: whether to keep extending the rate limit
         window every hit.
        :param amount: the number to increment by
        """
        raise NotImplementedError

    @abstractmethod
    def get(self, key: str) -> int:
        """
        :param key: the key to get the counter value for
        """
        raise NotImplementedError

    @abstractmethod
    def get_expiry(self, key: str) -> float:
        """
        :param key: the key to get the expiry for
        """
        raise NotImplementedError

    @abstractmethod
    def check(self) -> bool:
        """
        check if storage is healthy
        """
        raise NotImplementedError

    @abstractmethod
    def reset(self) -> Optional[int]:
        """
        reset storage to clear limits
        """
        raise NotImplementedError

    @abstractmethod
    def clear(self, key: str) -> None:
        """
        resets the rate limit key

        :param key: the key to clear rate limits for
        """
        raise NotImplementedError


class MovingWindowSupport(ABC):
    """
    Abstract base for storages that intend to support
    the moving window strategy
    """

    def __new__(cls, *args: Any, **kwargs: Any) -> MovingWindowSupport:  # type: ignore[misc]
        inst = super().__new__(cls)

        for method in {
            "acquire_entry",
            "get_moving_window",
        }:
            setattr(
                inst,
                method,
                _wrap_errors(cast(Storage, inst), getattr(inst, method)),
            )

        return inst

    @abstractmethod
    def acquire_entry(self, key: str, limit: int, expiry: int, amount: int = 1) -> bool:
        """
        :param key: rate limit key to acquire an entry in
        :param limit: amount of entries allowed
        :param expiry: expiry of the entry
        :param amount: the number of entries to acquire
        """
        raise NotImplementedError

    @abstractmethod
    def get_moving_window(self, key: str, limit: int, expiry: int) -> Tuple[float, int]:
        """
        returns the starting point and the number of entries in the moving
        window

        :param key: rate limit key
        :param expiry: expiry of entry
        :return: (start of window, number of acquired entries)
        """
        raise NotImplementedError


class SlidingWindowCounterSupport(ABC):
    """
    Abstract base for storages that intend to support
    the sliding window counter strategy
    """

    def __new__(cls, *args: Any, **kwargs: Any) -> SlidingWindowCounterSupport:  # type: ignore[misc]
        inst = super().__new__(cls)

        for method in {"acquire_sliding_window_entry", "get_sliding_window"}:
            setattr(
                inst,
                method,
                _wrap_errors(cast(Storage, inst), getattr(inst, method)),
            )

        return inst

    @abstractmethod
    def acquire_sliding_window_entry(
        self,
        previous_key: str,
        current_key: str,
        limit: int,
        expiry: int,
        amount: int = 1,
    ) -> bool:
        """
        Acquire an entry. Shift the current window to the previous window if it expired.
        :param current_window_key: current window key
        :param previous_window_key: previous window key
        :param limit: amount of entries allowed
        :param expiry: expiry of the entry
        :param amount: the number of entries to acquire
        """
        raise NotImplementedError

    @abstractmethod
    def get_sliding_window(
        self, previous_key: str, current_key: str
    ) -> tuple[int, float, int, float]:
        """
        Return the previous and current window information.
        This method should be implemented by the inherited classes if a more performant solution is available.
        Return a tuple[int, float, int_ float] with the following information:
        - previous window counter (int)
        - previous window TTL (float)
        - current window counter (int)
        - current window TTL (float)
        """
        raise NotImplementedError
