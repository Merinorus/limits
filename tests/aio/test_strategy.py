import time
from math import ceil

import pytest

from limits.aio.storage import MemcachedStorage
from limits.aio.strategies import (
    FixedWindowElasticExpiryRateLimiter,
    FixedWindowRateLimiter,
    MovingWindowRateLimiter,
    SlidingWindowCounterRateLimiter,
)
from limits.limits import (
    RateLimitItemPerHour,
    RateLimitItemPerMinute,
    RateLimitItemPerSecond,
)
from limits.storage import storage_from_string
from tests.utils import (
    async_all_storage,
    async_fixed_start,
    async_moving_window_storage,
    async_sliding_window_counter_storage,
    async_sliding_window_counter_timestamp_based_key,
    async_window,
    timestamp_based_key_ttl,
)


@pytest.mark.asyncio
class TestAsyncWindow:
    @async_all_storage
    @async_fixed_start
    async def test_fixed_window(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = FixedWindowRateLimiter(storage)
        limit = RateLimitItemPerSecond(10, 2)
        async with async_window(1) as (start, _):
            assert all([await limiter.hit(limit) for _ in range(0, 10)])
        assert not await limiter.hit(limit)
        assert (await limiter.get_window_stats(limit)).remaining == 0
        assert (await limiter.get_window_stats(limit)).reset_time == pytest.approx(
            start + 2, 1e-2
        )

    @async_all_storage
    @async_fixed_start
    async def test_fixed_window_empty_stats(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = FixedWindowRateLimiter(storage)
        limit = RateLimitItemPerSecond(10, 2)
        assert (await limiter.get_window_stats(limit)).remaining == 10
        assert (await limiter.get_window_stats(limit)).reset_time == pytest.approx(
            time.time(), 1e-2
        )

    @async_moving_window_storage
    async def test_moving_window_stats(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = MovingWindowRateLimiter(storage)
        limit = RateLimitItemPerMinute(2)
        assert await limiter.hit(limit, "key")
        time.sleep(1)
        assert await limiter.hit(limit, "key")
        time.sleep(1)
        assert not await limiter.hit(limit, "key")
        assert (await limiter.get_window_stats(limit, "key")).remaining == 0
        assert (
            await limiter.get_window_stats(limit, "key")
        ).reset_time - time.time() == pytest.approx(58, 1e-2)

    @async_all_storage
    @async_fixed_start
    async def test_fixed_window_multiple_cost(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = FixedWindowRateLimiter(storage)
        limit = RateLimitItemPerMinute(10, 2)
        assert not await limiter.hit(limit, "k1", cost=11)
        assert await limiter.hit(limit, "k2", cost=5)
        assert (await limiter.get_window_stats(limit, "k2")).remaining == 5
        assert not await limiter.test(limit, "k2", cost=6)
        assert not await limiter.hit(limit, "k2", cost=6)

    @async_all_storage
    @async_fixed_start
    async def test_fixed_window_with_elastic_expiry(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = FixedWindowElasticExpiryRateLimiter(storage)
        limit = RateLimitItemPerSecond(10, 2)
        async with async_window(1) as (start, end):
            assert all([await limiter.hit(limit) for _ in range(0, 10)])
            assert not await limiter.hit(limit)
        assert (await limiter.get_window_stats(limit)).remaining == 0
        assert (await limiter.get_window_stats(limit)).reset_time == pytest.approx(
            start + 2, 1e-2
        )
        async with async_window(3) as (start, end):
            assert not await limiter.hit(limit)
        assert await limiter.hit(limit)
        assert (await limiter.get_window_stats(limit)).remaining == 9
        assert (await limiter.get_window_stats(limit)).reset_time == pytest.approx(
            end + 2, 1e-2
        )

    @async_all_storage
    @async_fixed_start
    async def test_fixed_window_with_elastic_expiry_multiple_cost(
        self, uri, args, fixture
    ):
        storage = storage_from_string(uri, **args)
        limiter = FixedWindowElasticExpiryRateLimiter(storage)
        limit = RateLimitItemPerSecond(10, 2)
        assert not await limiter.hit(limit, "k1", cost=11)
        async with async_window(0) as (_, end):
            assert await limiter.hit(limit, "k2", cost=5)
        assert (await limiter.get_window_stats(limit, "k2")).remaining == 5
        assert (
            await limiter.get_window_stats(limit, "k2")
        ).reset_time == pytest.approx(end + 2, 1e-2)
        assert not await limiter.hit(limit, "k2", cost=6)

    @async_sliding_window_counter_storage
    @async_fixed_start
    async def test_sliding_window_counter(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = SlidingWindowCounterRateLimiter(storage)
        limit = RateLimitItemPerSecond(10, 2)
        if async_sliding_window_counter_timestamp_based_key(uri):
            # Avoid testing the behaviour when the window is about to be reset
            ttl = timestamp_based_key_ttl(limit)
            if ttl < 0.5:
                time.sleep(ttl)
        async with async_window(1) as (start, _):
            assert all([await limiter.hit(limit) for _ in range(0, 10)])
        assert not await limiter.hit(limit)
        assert (await limiter.get_window_stats(limit)).remaining == 0
        assert (await limiter.get_window_stats(limit)).reset_time == pytest.approx(
            start + 2, 1e-2
        )

    @async_sliding_window_counter_storage
    @async_fixed_start
    async def test_sliding_window_counter_empty_stats(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = SlidingWindowCounterRateLimiter(storage)
        limit = RateLimitItemPerSecond(10, 2)
        assert (await limiter.get_window_stats(limit)).remaining == 10
        assert (await limiter.get_window_stats(limit)).reset_time == pytest.approx(
            time.time(), 1e-2
        )

    @async_sliding_window_counter_storage
    @async_fixed_start
    async def test_sliding_window_counter_stats(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = SlidingWindowCounterRateLimiter(storage)
        limit = RateLimitItemPerMinute(2)
        if async_sliding_window_counter_timestamp_based_key(uri):
            next_second_from_now = ceil(time.time())
        assert await limiter.hit(limit, "key")
        time.sleep(1)
        assert await limiter.hit(limit, "key")
        time.sleep(1)
        assert not await limiter.hit(limit, "key")
        assert (await limiter.get_window_stats(limit, "key")).remaining == 0
        if async_sliding_window_counter_timestamp_based_key(uri):
            # With timestamp-based key implementation,
            # the reset time is periodic according to the worker's timestamp
            reset_time = (await limiter.get_window_stats(limit, "key")).reset_time
            expected_reset = int(
                limit.get_expiry() - (next_second_from_now % limit.get_expiry())
            )
            assert reset_time - next_second_from_now == pytest.approx(
                expected_reset, abs=1e-2
            )
        else:
            assert (
                await limiter.get_window_stats(limit, "key")
            ).reset_time - time.time() == pytest.approx(58, 1e-2)

    @async_sliding_window_counter_storage
    @async_fixed_start
    async def test_sliding_window_counter_multiple_cost(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = SlidingWindowCounterRateLimiter(storage)
        limit = RateLimitItemPerMinute(10, 2)
        if async_sliding_window_counter_timestamp_based_key(uri):
            # Avoid testing the behaviour when the window is about to be reset
            ttl = timestamp_based_key_ttl(limit)
            if ttl < 0.5:
                time.sleep(ttl)
        assert not await limiter.hit(limit, "k1", cost=11)
        assert await limiter.hit(limit, "k2", cost=5)
        assert (await limiter.get_window_stats(limit, "k2")).remaining == 5
        assert not await limiter.test(limit, "k2", cost=6)
        assert not await limiter.hit(limit, "k2", cost=6)

    @async_moving_window_storage
    async def test_moving_window(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = MovingWindowRateLimiter(storage)
        limit = RateLimitItemPerSecond(10, 2)

        # 5 hits in the first 100ms
        async with async_window(0.1):
            assert all([await limiter.hit(limit) for i in range(5)])
        # 5 hits in the last 100ms
        async with async_window(2, delay=1.8):
            assert all([await limiter.hit(limit) for i in range(5)])
            # 11th fails
            assert not await limiter.hit(limit)
        # 5 more succeed since there were only 5 in the last 2 seconds
        assert all([await limiter.hit(limit) for i in range(5)])
        assert (await limiter.get_window_stats(limit)).remaining == 0

    @async_moving_window_storage
    async def test_moving_window_empty_stats(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = MovingWindowRateLimiter(storage)
        limit = RateLimitItemPerSecond(10, 2)
        assert (await limiter.get_window_stats(limit)).remaining == 10
        assert (await limiter.get_window_stats(limit)).reset_time == pytest.approx(
            time.time() + 2, 1e-2
        )

    @async_moving_window_storage
    async def test_moving_window_multiple_cost(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = MovingWindowRateLimiter(storage)
        limit = RateLimitItemPerSecond(10, 2)

        assert not await limiter.hit(limit, "k1", cost=11)
        # 5 hits in the first 100ms
        async with async_window(0.1):
            assert await limiter.hit(limit, "k2", cost=5)
        # 5 hits in the last 100ms
        async with async_window(2, delay=1.8):
            assert all([await limiter.hit(limit, "k2") for i in range(4)])
            assert not await limiter.test(limit, "k2", cost=2)
            assert not await limiter.hit(limit, "k2", cost=2)
            assert await limiter.hit(limit, "k2")
        assert all([await limiter.hit(limit, "k2") for i in range(5)])
        assert (await limiter.get_window_stats(limit, "k2")).remaining == 0
        assert not await limiter.hit(limit, "k2", cost=2)

    @async_moving_window_storage
    async def test_moving_window_varying_cost(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = MovingWindowRateLimiter(storage)
        five_per_min = RateLimitItemPerMinute(5)
        await limiter.hit(five_per_min, cost=5)
        assert not await limiter.hit(five_per_min, cost=2)
        await limiter.clear(five_per_min)
        assert await limiter.hit(five_per_min)

    @async_moving_window_storage
    async def test_moving_window_huge_cost_async(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = MovingWindowRateLimiter(storage)
        many_per_min = RateLimitItemPerMinute(1_000_000)
        await limiter.hit(many_per_min, cost=999_999)
        assert not await limiter.hit(many_per_min, cost=2)
        await limiter.clear(many_per_min)
        assert await limiter.hit(many_per_min)

    @pytest.mark.memcached
    async def test_moving_window_memcached(self, memcached):
        storage = MemcachedStorage("memcached://localhost:22122")
        with pytest.raises(NotImplementedError):
            MovingWindowRateLimiter(storage)

    @async_all_storage
    @async_fixed_start
    @pytest.mark.flaky
    async def test_test_fixed_window(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = FixedWindowRateLimiter(storage)
        limit = RateLimitItemPerHour(2, 1)
        assert await limiter.hit(limit)
        assert await limiter.test(limit)
        assert await limiter.hit(limit)
        assert not await limiter.test(limit)
        assert not await limiter.hit(limit)

    @async_moving_window_storage
    async def test_test_moving_window(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limit = RateLimitItemPerHour(2, 1)
        limiter = MovingWindowRateLimiter(storage)
        assert await limiter.hit(limit)
        assert await limiter.test(limit)
        assert await limiter.hit(limit)
        assert not await limiter.test(limit)
        assert not await limiter.hit(limit)
