"""
Asynchronous rate limiting strategies
"""

import time
from abc import ABC, abstractmethod
from typing import cast

from limits.aio.storage.base import SlidingWindowCounterSupport

from ..limits import RateLimitItem
from ..storage import StorageTypes
from ..util import WindowStats
from .storage import MovingWindowSupport, Storage


class RateLimiter(ABC):
    def __init__(self, storage: StorageTypes):
        assert isinstance(storage, Storage)
        self.storage: Storage = storage

    @abstractmethod
    async def hit(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Consume the rate limit

        :param item: the rate limit item
        :param identifiers: variable list of strings to uniquely identify the
         limit
        :param cost: The cost of this hit, default 1
        """
        raise NotImplementedError

    @abstractmethod
    async def test(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Check if the rate limit can be consumed

        :param item: the rate limit item
        :param identifiers: variable list of strings to uniquely identify the
         limit
        :param cost: The expected cost to be consumed, default 1
        """
        raise NotImplementedError

    @abstractmethod
    async def get_window_stats(
        self, item: RateLimitItem, *identifiers: str
    ) -> WindowStats:
        """
        Query the reset time and remaining amount for the limit

        :param item: the rate limit item
        :param identifiers: variable list of strings to uniquely identify the
         limit
        :return: (reset time, remaining))
        """
        raise NotImplementedError

    async def clear(self, item: RateLimitItem, *identifiers: str) -> None:
        return await self.storage.clear(item.key_for(*identifiers))


class MovingWindowRateLimiter(RateLimiter):
    """
    Reference: :ref:`strategies:moving window`
    """

    def __init__(self, storage: StorageTypes) -> None:
        if not (
            hasattr(storage, "acquire_entry") or hasattr(storage, "get_moving_window")
        ):
            raise NotImplementedError(
                "MovingWindowRateLimiting is not implemented for storage "
                "of type %s" % storage.__class__
            )
        super().__init__(storage)

    async def hit(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Consume the rate limit

        :param item: the rate limit item
        :param identifiers: variable list of strings to uniquely identify the
         limit
        :param cost: The cost of this hit, default 1
        """

        return await cast(MovingWindowSupport, self.storage).acquire_entry(
            item.key_for(*identifiers), item.amount, item.get_expiry(), amount=cost
        )

    async def test(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Check if the rate limit can be consumed

        :param item: the rate limit item
        :param identifiers: variable list of strings to uniquely identify the
         limit
        :param cost: The expected cost to be consumed, default 1
        """
        res = await cast(MovingWindowSupport, self.storage).get_moving_window(
            item.key_for(*identifiers),
            item.amount,
            item.get_expiry(),
        )
        amount = res[1]

        return amount <= item.amount - cost

    async def get_window_stats(
        self, item: RateLimitItem, *identifiers: str
    ) -> WindowStats:
        """
        returns the number of requests remaining within this limit.

        :param item: the rate limit item
        :param identifiers: variable list of strings to uniquely identify the
         limit
        :return: (reset time, remaining)
        """
        window_start, window_items = await cast(
            MovingWindowSupport, self.storage
        ).get_moving_window(item.key_for(*identifiers), item.amount, item.get_expiry())
        reset = window_start + item.get_expiry()

        return WindowStats(reset, item.amount - window_items)


class FixedWindowRateLimiter(RateLimiter):
    """
    Reference: :ref:`strategies:fixed window`
    """

    async def hit(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Consume the rate limit

        :param item: the rate limit item
        :param identifiers: variable list of strings to uniquely identify the
         limit
        :param cost: The cost of this hit, default 1
        """

        return (
            await self.storage.incr(
                item.key_for(*identifiers),
                item.get_expiry(),
                elastic_expiry=False,
                amount=cost,
            )
            <= item.amount
        )

    async def test(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Check if the rate limit can be consumed

        :param item: the rate limit item
        :param identifiers: variable list of strings to uniquely identify the
         limit
        :param cost: The expected cost to be consumed, default 1
        """

        return (
            await self.storage.get(item.key_for(*identifiers)) < item.amount - cost + 1
        )

    async def get_window_stats(
        self, item: RateLimitItem, *identifiers: str
    ) -> WindowStats:
        """
        Query the reset time and remaining amount for the limit

        :param item: the rate limit item
        :param identifiers: variable list of strings to uniquely identify the
         limit
        :return: reset time, remaining
        """
        remaining = max(
            0,
            item.amount - await self.storage.get(item.key_for(*identifiers)),
        )
        reset = await self.storage.get_expiry(item.key_for(*identifiers))

        return WindowStats(reset, remaining)


class SlidingWindowCounterRateLimiter(RateLimiter):
    """
    Reference: :ref:`strategies:sliding window counter`
    """

    def __init__(self, storage: StorageTypes):
        if not hasattr(storage, "get_sliding_window") or not hasattr(
            storage, "acquire_sliding_window_entry"
        ):
            raise NotImplementedError(
                "SlidingWindowCounterRateLimiting is not implemented for storage "
                "of type %s" % storage.__class__
            )
        super().__init__(storage)

    def _weighted_count(
        self,
        item: RateLimitItem,
        previous_count: int,
        previous_expires_in: float,
        current_count: int,
    ) -> int:
        """
        Return the approximated by weighting the previous window count and adding the current window count.
        """
        return round(
            previous_count * previous_expires_in / item.get_expiry() + current_count
        )

    def _current_key_for(self, item: RateLimitItem, *identifiers: str) -> str:
        """
        Return the current window's storage key.

        Contrary to other strategies that have one key per rate limit item,
        this strategy has two keys per rate limit item than must be on the same machine.
        To keep the current key and the previous key on the same Redis cluster node,
        curvy braces are added.

        Eg: "{constructed_key}"
        """
        return f"{{{item.key_for(*identifiers)}}}"

    def _previous_key_for(self, item: RateLimitItem, *identifiers: str) -> str:
        """
        Return the previous window's storage key.

        Curvy braces are added on the common pattern with the current window's key,
        so the current and the previous key are stored on the same Redis cluster node.

        Eg: "{constructed_key}/-1"
        """
        return f"{self._current_key_for(item, *identifiers)}/-1"

    async def hit(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Consume the rate limit

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :param cost: The cost of this hit, default 1
        """
        return await cast(
            SlidingWindowCounterSupport, self.storage
        ).acquire_sliding_window_entry(
            self._previous_key_for(item, *identifiers),
            self._current_key_for(item, *identifiers),
            item.amount,
            item.get_expiry(),
            cost,
        )

    async def test(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Check if the rate limit can be consumed

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :param cost: The expected cost to be consumed, default 1
        """

        previous_count, previous_expires_in, current_count, _ = await cast(
            SlidingWindowCounterSupport, self.storage
        ).get_sliding_window(
            self._previous_key_for(item, *identifiers),
            self._current_key_for(item, *identifiers),
        )

        return (
            self._weighted_count(
                item, previous_count, previous_expires_in, current_count
            )
            < item.amount - cost + 1
        )

    async def get_window_stats(
        self, item: RateLimitItem, *identifiers: str
    ) -> WindowStats:
        """
        Query the reset time and remaining amount for the limit.

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :return: (reset time, remaining)
        """

        (
            previous_count,
            previous_expires_in,
            current_count,
            current_expires_in,
        ) = await cast(SlidingWindowCounterSupport, self.storage).get_sliding_window(
            self._previous_key_for(item, *identifiers),
            self._current_key_for(item, *identifiers),
        )
        remaining = max(
            0,
            item.amount
            - self._weighted_count(
                item, previous_count, previous_expires_in, current_count
            ),
        )
        return WindowStats(
            time.time() + current_expires_in - item.get_expiry(), remaining
        )


class FixedWindowElasticExpiryRateLimiter(FixedWindowRateLimiter):
    """
    Reference: :ref:`strategies:fixed window with elastic expiry`
    """

    async def hit(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Consume the rate limit

        :param item: a :class:`limits.limits.RateLimitItem` instance
        :param identifiers: variable list of strings to uniquely identify the
         limit
        :param cost: The cost of this hit, default 1
        """
        amount = await self.storage.incr(
            item.key_for(*identifiers),
            item.get_expiry(),
            elastic_expiry=True,
            amount=cost,
        )

        return amount <= item.amount


STRATEGIES = {
    "sliding-window-counter": SlidingWindowCounterRateLimiter,
    "fixed-window": FixedWindowRateLimiter,
    "fixed-window-elastic-expiry": FixedWindowElasticExpiryRateLimiter,
    "moving-window": MovingWindowRateLimiter,
}
