"""
Rate limiting strategies
"""

import time
from abc import ABCMeta, abstractmethod
from math import ceil
from typing import Dict, Type, Union, cast

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


class ApproximatedMovingWindowRateLimiter(RateLimiter):
    """
    TODO doc

    Use two fixed windows: the current one and the previous one.
    """

    def get_approximated_count(self, item: RateLimitItem, *identifiers: str) -> int:
        """
        Get the approximated count by averaging the current window and the previous one,
        depending on how much time passed since the new window has been created.
        The result is rounded up to the next whole number.

        Args:
            item (RateLimitItem): The rate limit item
        """
        current_count = self.storage.get(item.key_for(*identifiers))
        previous_count = self.storage.get(item.previous_key_for(*identifiers))
        current_window_expire_in = max(
            0, self.storage.get_expiry(item.key_for(*identifiers)) - time.time()
        )

        approximated_count = ceil(
            previous_count
            * (current_window_expire_in - item.get_expiry())
            / item.get_expiry()
            + current_count
        )
        return approximated_count

    def hit(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Consume the rate limit

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :param cost: The cost of this hit, default 1
        """
        current_window_expire_in = max(
            0, self.storage.get_expiry(item.key_for(*identifiers)) - time.time()
        )
        # print(f"Current window expires in {current_window_expire_in} seconds")
        if (
            current_window_expire_in > 0
            and current_window_expire_in <= item.get_expiry()
        ):
            # Current window time elapsed, move the counter to the previous window.
            print(
                "Current window time elapsed, move the counter to the previous window."
            )
            self.storage.incr(
                item.previous_key_for(*identifiers),
                expiry=ceil(current_window_expire_in),
                amount=self.storage.get(item.key_for(*identifiers)),
            )
            self.storage.clear(item.key_for(*identifiers))

        has_previous_window = self.storage.get(item.previous_key_for(*identifiers))
        if has_previous_window:
            previous_windows_expire_in = max(
                0,
                self.storage.get_expiry(item.previous_key_for(*identifiers))
                - time.time(),
            )
            expiry = item.get_expiry() + ceil(previous_windows_expire_in)
        else:
            expiry = item.get_expiry() * 2

        self.storage.incr(
            item.key_for(*identifiers), expiry, elastic_expiry=False, amount=cost
        )

        print("Counter incremented.")
        previous_window_expire_in = max(
            0,
            self.storage.get_expiry(item.previous_key_for(*identifiers)) - time.time(),
        )
        current_window_expire_in = max(
            0, self.storage.get_expiry(item.key_for(*identifiers)) - time.time()
        )
        print(f"previous window expires: {previous_window_expire_in}")
        print(f"current  window expires: {current_window_expire_in}")
        print(
            f"previous window counter: {self.storage.get(item.previous_key_for(*identifiers))}"
        )
        print(
            f"current  window counter: {self.storage.get(item.key_for(*identifiers))}"
        )
        print(
            f"Approximated    counter: {self.get_approximated_count(item, *identifiers)}"
        )

        return self.get_approximated_count(item, *identifiers) <= item.amount

    def test(self, item: RateLimitItem, *identifiers: str, cost: int = 1) -> bool:
        """
        Check if the rate limit can be consumed

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :param cost: The expected cost to be consumed, default 1
        """

        return self.get_approximated_count(item, *identifiers) < item.amount - cost + 1

    def get_window_stats(self, item: RateLimitItem, *identifiers: str) -> WindowStats:
        """
        Query the reset time and remaining amount for the limit.

        :param item: The rate limit item
        :param identifiers: variable list of strings to uniquely identify this
         instance of the limit
        :return: (reset time, remaining)
        """
        reset = self.storage.get_expiry(item.key_for(*identifiers))
        remaining = max(
            0, item.amount - self.get_approximated_count(item, *identifiers)
        )

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
    Type[ApproximatedMovingWindowRateLimiter],
    Type[FixedWindowRateLimiter],
    Type[FixedWindowElasticExpiryRateLimiter],
    Type[MovingWindowRateLimiter],
]

STRATEGIES: Dict[str, KnownStrategy] = {
    "approximated-moving-window": ApproximatedMovingWindowRateLimiter,
    "fixed-window": FixedWindowRateLimiter,
    "fixed-window-elastic-expiry": FixedWindowElasticExpiryRateLimiter,
    "moving-window": MovingWindowRateLimiter,
}
