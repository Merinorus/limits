"""
Rate limiting strategies
"""

import time
from abc import ABCMeta, abstractmethod
from math import floor
from typing import Dict, Type, Union, cast

from limits.storage.base import SlidingWindowCounterSupport

from .limits import RateLimitItem
from .storage import MovingWindowSupport, Storage, StorageTypes
from .util import WindowStats


class RateLimiter(metaclass=ABCMeta):
    def __init__(self, storage: StorageTypes):
        assert isinstance(storage, Storage)
        self.storage: Storage = storage

    @abstractmethod
    def hit(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Consume the rate limit

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :param cost: The cost of this hit, default 1
        """
        raise NotImplementedError

    @abstractmethod
    def test(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Check the rate limit without consuming from it.

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
          instance of the limit
        :param cost: The expected cost to be consumed, default 1
        """
        raise NotImplementedError

    @abstractmethod
    def get_window_stats(self, item: RateLimitItem, *identifiers: str) -> WindowStats:
        """
        Query the reset time and remaining amount for the limit

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :return: (reset time, remaining)
        """
        raise NotImplementedError

    def clear(self, item: RateLimitItem, *identifiers: str) -> None:
        return self.storage.clear(item.key_for(*identifiers))


class MovingWindowRateLimiter(RateLimiter):
    """
    Reference: :ref:`strategies:moving window`
    """

    def __init__(self, storage: StorageTypes):
        if not (
            hasattr(storage, "acquire_entry") or hasattr(storage, "get_moving_window")
        ):
            raise NotImplementedError(
                "MovingWindowRateLimiting is not implemented for storage "
                "of type %s" % storage.__class__
            )
        super().__init__(storage)

    def hit(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Consume the rate limit

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :param cost: The cost of this hit, default 1
        :return: (reset time, remaining)
        """

        return cast(MovingWindowSupport, self.storage).acquire_entry(
            item.key_for(*identifiers), item.amount, item.get_expiry(), amount=cost
        )

    def test(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Check if the rate limit can be consumed

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :param cost: The expected cost to be consumed, default 1
        """

        return (
            cast(MovingWindowSupport, self.storage).get_moving_window(
                item.key_for(*identifiers),
                item.amount,
                item.get_expiry(),
            )[1]
            <= item.amount - cost
        )

    def get_window_stats(self, item: RateLimitItem, *identifiers: str) -> WindowStats:
        """
        returns the number of requests remaining within this limit.

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :return: tuple (reset time, remaining)
        """
        window_start, window_items = cast(
            MovingWindowSupport, self.storage
        ).get_moving_window(item.key_for(*identifiers), item.amount, item.get_expiry())
        reset = window_start + item.get_expiry()

        return WindowStats(reset, item.amount - window_items)


class FixedWindowRateLimiter(RateLimiter):
    """
    Reference: :ref:`strategies:fixed window`
    """

    def hit(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Consume the rate limit

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :param cost: The cost of this hit, default 1
        """

        return (
            self.storage.incr(
                item.key_for(*identifiers),
                item.get_expiry(),
                elastic_expiry=False,
                amount=cost,
            )
            <= item.amount
        )

    def test(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Check if the rate limit can be consumed

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :param cost: The expected cost to be consumed, default 1
        """

        return self.storage.get(item.key_for(*identifiers)) < item.amount - cost + 1

    def get_window_stats(self, item: RateLimitItem, *identifiers: str) -> WindowStats:
        """
        Query the reset time and remaining amount for the limit

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :return: (reset time, remaining)
        """
        remaining = max(0, item.amount - self.storage.get(item.key_for(*identifiers)))
        reset = self.storage.get_expiry(item.key_for(*identifiers))

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
    ) -> float:
        """
        Return the approximated by weighting the previous window count and adding the current window count.
        """
        return previous_count * previous_expires_in / item.get_expiry() + current_count

    # def _current_key_for(self, item: RateLimitItem, *identifiers: str) -> str:
    #     """
    #     Return the current window's storage key.

    #     Contrary to other strategies that have one key per rate limit item,
    #     this strategy has two keys per rate limit item than must be on the same machine.
    #     To keep the current key and the previous key on the same Redis cluster node,
    #     curvy braces are added.

    #     Eg: "{constructed_key}"
    #     """
    #     return f"{{{item.key_for(*identifiers)}}}"

    # def _previous_key_for(self, item: RateLimitItem, *identifiers: str) -> str:
    #     """
    #     Return the previous window's storage key.

    #     Curvy braces are added on the common pattern with the current window's key,
    #     so the current and the previous key are stored on the same Redis cluster node.

    #     Eg: "{constructed_key}/-1"
    #     """
    #     return f"{self._current_key_for(item, *identifiers)}/-1"

    def hit(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Consume the rate limit

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :param cost: The cost of this hit, default 1
        """
        # print("HIT")
        return cast(
            SlidingWindowCounterSupport, self.storage
        ).acquire_sliding_window_entry(
            item.key_for(*identifiers),
            item.amount,
            item.get_expiry(),
            cost,
        )

    def test(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Check if the rate limit can be consumed

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :param cost: The expected cost to be consumed, default 1
        """
        # print("TEST")
        previous_count, previous_expires_in, current_count, _ = cast(
            SlidingWindowCounterSupport, self.storage
        ).get_sliding_window(item.key_for(*identifiers), item.get_expiry())

        return (
            self._weighted_count(
                item, previous_count, previous_expires_in, current_count
            )
            < item.amount - cost + 1
        )

    def get_window_stats(self, item: RateLimitItem, *identifiers: str) -> WindowStats:
        """
        Query the reset time and remaining amount for the limit.

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :return: (reset time, remaining)
        """
        # print("GET WINDOW STATS")

        previous_count, previous_expires_in, current_count, current_expires_in = cast(
            SlidingWindowCounterSupport, self.storage
        ).get_sliding_window(item.key_for(*identifiers), item.get_expiry())
        remaining = max(
            0,
            item.amount
            - floor(
                self._weighted_count(
                    item, previous_count, previous_expires_in, current_count
                )
            ),
        )
        now = time.time()
        # print(f"previous = {previous_count}, current = {current_count}")
        if previous_count >= 1 and current_count == 0:
            # print("A")
            previous_window_reset_period = item.get_expiry() / previous_count
            # print(f"previous_window_reset_period={previous_window_reset_period}")
            # window_reset_half_period = window_reset_period / 2
            # reset = window_reset_period - (now % window_reset_period) + now
            # weighted_previous_count = (previous_count * previous_expires_in / item.get_expiry())
            # print(f"weighted_previous_count: {weighted_previous_count}")
            # print(f"window reset period: {window_reset_period} s")
            # print(f"previous_expires_in: {previous_expires_in}")
            # reset = previous_expires_in % window_reset_period + now
            # reset = (previous_expires_in - window_reset_half_period) % window_reset_period + now
            reset = previous_expires_in % previous_window_reset_period + now
            # print(f"reset in: {reset - now} ")
        elif previous_count >= 1 and current_count >= 1:
            # print("B")
            # print(f"weighted_count: {weighted_count}")
            previous_window_reset_period = item.get_expiry() / previous_count
            # previous_reset = previous_window_reset_period - (now % previous_window_reset_period) + now
            # Previous window will reset before the current one
            previous_reset = previous_expires_in % previous_window_reset_period + now
            current_reset = current_expires_in % item.get_expiry() + now
            reset = min(previous_reset, current_reset)
        elif previous_count == 0 and current_count >= 1:
            # print("C")
            # window_reset_period = item.get_expiry() / current_count
            reset = current_expires_in % item.get_expiry() + now
            # print(f"reset in: {reset - time.time()} s")
        else:
            # print("D")
            # previous_count == 0 and current_count == 0
            reset = now
        # if previous_count >= 1:
        #     # print(f"previous window = {previous_count}")
        #     window_reset_period = item.get_expiry() / (previous_count + 1)
        #     # print(f"previous reset window interval: {previous_window_reset_period}")
        #     # reset = previous_window_reset_period - (now % previous_window_reset_period) + now
        #     reset = (
        #         min(
        #             previous_expires_in % window_reset_period,
        #             current_expires_in % item.get_expiry(),
        #         )
        #         + now
        #     )
        # else:
        #     # print("current window only")
        #     reset = current_expires_in % item.get_expiry() + now
        #     if current_count >= 1:
        #         window_reset_period = item.get_expiry() / (current_count + 1)
        #         reset = reset + window_reset_period
        # reset = current_expires_in % item.get_expiry() + now

        # if previous_count >= 1:
        #     window_reset_period = item.get_expiry() / (previous_count + 1)
        #     reset = (
        #         min(
        #             previous_expires_in % window_reset_period,
        #             current_expires_in % item.get_expiry(),
        #         )
        #         + now
        #     )
        # if current_count >= 1:
        #     window_reset_period = item.get_expiry() / (current_count + 1)
        #     reset = reset + window_reset_period

        # print(f"previous count: {previous_count}")
        # print(f"current  count: {current_count}")
        # print(
        #     f"weighted count: {self._weighted_count(item, previous_count, previous_expires_in, current_count)}"
        # )
        # print(f"remaining     : {remaining}")
        # print(f"previous expires in  : {previous_expires_in} s")
        # print(f"current expires in   : {current_expires_in} s")
        # print(f"rate limiter reset in: {reset - now} s")
        # print(f"rate limiter reset ts: {reset} (timestamp in seconds since epoch)")
        return WindowStats(reset, remaining)


class FixedWindowElasticExpiryRateLimiter(FixedWindowRateLimiter):
    """
    Reference: :ref:`strategies:fixed window with elastic expiry`
    """

    def hit(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Consume the rate limit

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :param cost: The cost of this hit, default 1
        """

        return (
            self.storage.incr(
                item.key_for(*identifiers),
                item.get_expiry(),
                elastic_expiry=True,
                amount=cost,
            )
            <= item.amount
        )


KnownStrategy = Union[
    Type[SlidingWindowCounterRateLimiter],
    Type[FixedWindowRateLimiter],
    Type[FixedWindowElasticExpiryRateLimiter],
    Type[MovingWindowRateLimiter],
]

STRATEGIES: Dict[str, KnownStrategy] = {
    "sliding-window-counter": SlidingWindowCounterRateLimiter,
    "fixed-window": FixedWindowRateLimiter,
    "fixed-window-elastic-expiry": FixedWindowElasticExpiryRateLimiter,
    "moving-window": MovingWindowRateLimiter,
}
