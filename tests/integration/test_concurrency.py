import asyncio
import random
import threading
import time
from math import ceil
from uuid import uuid4

import pytest

import limits.aio.storage.memory
import limits.aio.strategies
import limits.strategies
from limits.limits import RateLimitItemPerMinute, RateLimitItemPerSecond
from limits.storage import storage_from_string
from tests.utils import (
    all_storage,
    async_all_storage,
    async_moving_window_storage,
    async_sliding_window_counter_storage,
    async_sliding_window_counter_timestamp_based_key,
    moving_window_storage,
    sliding_window_counter_storage,
    timestamp_based_key_ttl,
)


@pytest.mark.integration
class TestConcurrency:
    @all_storage
    def test_fixed_window(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = limits.strategies.FixedWindowRateLimiter(storage)
        limit = RateLimitItemPerMinute(5)

        [limiter.hit(limit, uuid4().hex) for _ in range(50)]

        key = uuid4().hex
        hits = []

        def hit():
            time.sleep(random.random())
            if limiter.hit(limit, key):
                hits.append(None)

        threads = [threading.Thread(target=hit) for _ in range(50)]
        [t.start() for t in threads]
        [t.join() for t in threads]

        assert len(hits) == 5

    @sliding_window_counter_storage
    def test_sliding_window_counter(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = limits.strategies.SlidingWindowCounterRateLimiter(storage)
        limit = RateLimitItemPerMinute(5)

        [limiter.hit(limit, uuid4().hex) for _ in range(100)]

        key = uuid4().hex
        hits = []

        def hit():
            time.sleep(random.random() / 1000)
            if limiter.hit(limit, key):
                hits.append(None)

        threads = [threading.Thread(target=hit) for _ in range(100)]
        [t.start() for t in threads]
        [t.join() for t in threads]

        assert len(hits) == 5

    @moving_window_storage
    def test_moving_window(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = limits.strategies.MovingWindowRateLimiter(storage)
        limit = RateLimitItemPerMinute(5)

        [limiter.hit(limit, uuid4().hex) for _ in range(50)]

        key = uuid4().hex
        hits = []

        def hit():
            time.sleep(random.random())
            if limiter.hit(limit, key):
                hits.append(None)

        threads = [threading.Thread(target=hit) for _ in range(50)]
        [t.start() for t in threads]
        [t.join() for t in threads]

        assert len(hits) == 5


@pytest.mark.asyncio
@pytest.mark.integration
class TestAsyncConcurrency:
    @async_all_storage
    async def test_fixed_window(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = limits.aio.strategies.FixedWindowRateLimiter(storage)
        limit = RateLimitItemPerMinute(5)

        [await limiter.hit(limit, uuid4().hex) for _ in range(50)]

        key = uuid4().hex
        hits = []

        async def hit():
            await asyncio.sleep(random.random())
            if await limiter.hit(limit, key):
                hits.append(None)

        await asyncio.gather(*[hit() for _ in range(50)])

        assert len(hits) == 5

    @async_sliding_window_counter_storage
    async def test_sliding_window_counter(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = limits.aio.strategies.SlidingWindowCounterRateLimiter(storage)
        limit = RateLimitItemPerMinute(5)
        if async_sliding_window_counter_timestamp_based_key(uri):
            # Avoid testing the behaviour when the window is about to be reset
            ttl = timestamp_based_key_ttl(limit)
            if ttl < 1:
                time.sleep(ttl)

        key = uuid4().hex
        hits = []

        async def hit():
            await asyncio.sleep(random.random() / 1000)
            if await limiter.hit(limit, key):
                hits.append(None)

        await asyncio.gather(*[hit() for _ in range(100)])

        assert len(hits) == 5

    @async_sliding_window_counter_storage
    @pytest.mark.flaky(max_runs=3)
    async def test_sliding_window_counter_shift(self, uri, args, fixture):
        """Check that the window is shifted only once under high concurrency"""
        storage = storage_from_string(uri, **args)
        limiter = limits.aio.strategies.SlidingWindowCounterRateLimiter(storage)
        limiter_amount = 20
        limiter_seconds = 2
        limit = RateLimitItemPerSecond(limiter_amount, limiter_seconds)
        if async_sliding_window_counter_timestamp_based_key(uri):
            # Avoid testing the behaviour when the window is about to be reset
            ttl = timestamp_based_key_ttl(limit)
            if ttl < 0.5:
                time.sleep(ttl)

        key = uuid4().hex
        hits = []

        async def hit():
            await asyncio.sleep(random.random() / 1000)
            if await limiter.hit(limit, key):
                hits.append(None)

        async def get_window_stats():
            await asyncio.sleep(random.random() / 1000)
            await limiter.get_window_stats(limit, key)

        await asyncio.gather(*[hit() for _ in range(limiter_amount)])
        assert len(hits) == limiter_amount
        assert (await limiter.get_window_stats(limit, key)).remaining == 0

        reset_time = (await limiter.get_window_stats(limit, key)).reset_time
        print(f"reset in: {reset_time - time.time()}")
        offset_before_reset = 0.01
        print("sleeping...")
        time.sleep(reset_time - time.time() - offset_before_reset)
        reset_time = (await limiter.get_window_stats(limit, key)).reset_time
        print(f"reset in: {reset_time - time.time()}")
        # await asyncio.gather(*[get_window_stats() for _ in range(500)], *[hit() for _ in range(500)])
        await asyncio.gather(
            *[get_window_stats() for _ in range(20)], *[hit() for _ in range(20)]
        )
        # If the window has shifted, only one more hit should be allowed
        t1 = time.time()
        elapsed_time_since_reset = t1 - reset_time
        print(f"Elapsed time since reset: {elapsed_time_since_reset}")
        if elapsed_time_since_reset % (limiter_seconds / limiter_amount) <= 0.01:
            # If the previous window has just moved to the next subperiod,
            # the limiter might be not hit enough
            additional_hit = await hit()
            print(
                "Test finished too close to a subwindow period reset. Trying one more hit"
            )
            print(f"Additional hit: {additional_hit}")
            t1 = time.time()
        additional_hits = ceil((t1 - reset_time) / (limiter_seconds / limiter_amount))
        print(f"expected additional hits: {additional_hits}")
        if len(hits) < limiter_amount + additional_hits:
            # One hit might be missing depending on how close to a subperiod the test finishes
            missing_hits = limiter_amount + additional_hits - len(hits)
            await asyncio.gather(*[hit() for _ in range(missing_hits)])
            print(f"{missing_hits} hits were missing")
        assert (
            len(hits) == limiter_amount + additional_hits
            or len(hits) == limiter_amount + additional_hits - 1
        )

    @async_moving_window_storage
    async def test_moving_window(self, uri, args, fixture):
        storage = storage_from_string(uri, **args)
        limiter = limits.aio.strategies.MovingWindowRateLimiter(storage)
        limit = RateLimitItemPerMinute(5)

        [await limiter.hit(limit, uuid4().hex) for _ in range(50)]

        key = uuid4().hex
        hits = []

        async def hit():
            await asyncio.sleep(random.random())
            if await limiter.hit(limit, key):
                hits.append(None)

        await asyncio.gather(*[hit() for _ in range(50)])

        assert len(hits) == 5
