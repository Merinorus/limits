import time
import urllib.parse
from contextlib import asynccontextmanager
from math import ceil
from typing import AsyncGenerator, Iterable

from deprecated.sphinx import versionadded

from limits.aio.storage.base import SlidingWindowCounterSupport, Storage
from limits.typing import EmcacheClientP, ItemP, Optional, Tuple, Type, Union


@versionadded(version="2.1")
class MemcachedStorage(Storage, SlidingWindowCounterSupport):
    """
    Rate limit storage with memcached as backend.

    Depends on :pypi:`emcache`
    """

    STORAGE_SCHEME = ["async+memcached"]
    """The storage scheme for memcached to be used in an async context"""

    DEPENDENCIES = ["emcache"]

    def __init__(
        self,
        uri: str,
        wrap_exceptions: bool = False,
        **options: Union[float, str, bool],
    ) -> None:
        """
        :param uri: memcached location of the form
         ``async+memcached://host:port,host:port``
        :param wrap_exceptions: Whether to wrap storage exceptions in
         :exc:`limits.errors.StorageError` before raising it.
        :param options: all remaining keyword arguments are passed
         directly to the constructor of :class:`emcache.Client`
        :raise ConfigurationError: when :pypi:`emcache` is not available
        """
        parsed = urllib.parse.urlparse(uri)
        self.hosts = []

        for host, port in (
            loc.split(":") for loc in parsed.netloc.strip().split(",") if loc.strip()
        ):
            self.hosts.append((host, int(port)))

        self._options = options
        self._storage = None
        super().__init__(uri, wrap_exceptions=wrap_exceptions, **options)
        self.dependency = self.dependencies["emcache"].module

    @property
    def base_exceptions(
        self,
    ) -> Union[Type[Exception], Tuple[Type[Exception], ...]]:  # pragma: no cover
        return (
            self.dependency.ClusterNoAvailableNodes,
            self.dependency.CommandError,
        )

    async def get_storage(self) -> EmcacheClientP:
        if not self._storage:
            self._storage = await self.dependency.create_client(
                [self.dependency.MemcachedHostAddress(h, p) for h, p in self.hosts],
                **self._options,
            )
        assert self._storage
        return self._storage

    async def get(self, key: str) -> int:
        """
        :param key: the key to get the counter value for
        """

        item = await (await self.get_storage()).get(key.encode("utf-8"))

        return item and int(item.value) or 0

    async def get_many(self, keys: Iterable[str]) -> dict[bytes, ItemP]:
        """
        Return multiple counters at once
        :param key: the key to get the counter value for
        """
        return await (await self.get_storage()).get_many(
            [k.encode("utf-8") for k in keys]
        )

    async def clear(self, key: str) -> None:
        """
        :param key: the key to clear rate limits for
        """
        await (await self.get_storage()).delete(key.encode("utf-8"))

    async def set(self, key: str, value: int, expiry: float) -> None:
        """
        set the counter for a given rate limit key to the specificied amount
        :param key: the key to set
        :param expiry: amount in seconds for the key to expire in
         window every hit.
        :param amount: the number to set to
        """
        storage = await self.get_storage()
        await storage.set(key.encode("utf-8"), bytes(value), exptime=ceil(expiry))
        await storage.set(
            self._expiration_key(key).encode("utf-8"),
            str(expiry + time.time()).encode("utf-8"),
            exptime=ceil(expiry),
        )

    async def touch(self, key: str, expiry: float) -> bool:
        """
        set the a new expiry for a given counter
        :param key: the key to set
        :param expiry: the new amount in seconds for the key to expire
        """
        storage = await self.get_storage()
        succeed = True
        try:
            await storage.touch(key.encode("utf-8"), exptime=ceil(expiry))
        except self.dependency.NotStoredStorageCommandError:
            succeed = False
        if succeed:
            await storage.set(
                self._expiration_key(key).encode("utf-8"),
                str(expiry + time.time()).encode("utf-8"),
                exptime=ceil(expiry),
            )
        return succeed

    async def incr(
        self, key: str, expiry: float, elastic_expiry: bool = False, amount: int = 1
    ) -> int:
        """
        increments the counter for a given rate limit key

        :param key: the key to increment
        :param expiry: amount in seconds for the key to expire in
        :param elastic_expiry: whether to keep extending the rate limit
         window every hit.
        :param amount: the number to increment by
        """
        storage = await self.get_storage()
        limit_key = key.encode("utf-8")
        expire_key = self._expiration_key(key).encode()
        added = True
        try:
            await storage.add(limit_key, f"{amount}".encode(), exptime=ceil(expiry))
        except self.dependency.NotStoredStorageCommandError:
            added = False
            storage = await self.get_storage()

        if not added:
            value = await storage.increment(limit_key, amount) or amount

            if elastic_expiry:
                await storage.touch(limit_key, exptime=ceil(expiry))
                await storage.set(
                    expire_key,
                    str(expiry + time.time()).encode("utf-8"),
                    exptime=ceil(expiry),
                    noreply=False,
                )

            return value
        else:
            await storage.set(
                expire_key,
                str(expiry + time.time()).encode("utf-8"),
                exptime=ceil(expiry),
                noreply=False,
            )

        return amount

    async def get_expiry(self, key: str) -> float:
        """
        :param key: the key to get the expiry for
        """
        storage = await self.get_storage()
        item = await storage.get(self._expiration_key(key).encode("utf-8"))

        return item and float(item.value) or time.time()

    def _ttl(
        self, expiry: Optional[Union[float, bytes]], now: Optional[float] = None
    ) -> float:
        if now is None:
            now = time.time()
        result = max(0, float(expiry or 0) - time.time())
        return result

    async def get_ttl(self, key: str) -> float:
        """
        :param key: the key to get the TTL for
        """
        key_expires_at = await self.get_expiry(key)
        return self._ttl(key_expires_at)

    def _expiration_key(self, key: str) -> str:
        """
        Return the expiration key for the given counter key.

        Memcached doesn't natively return the expiration time or TTL for a given key,
        so we implement the expiration time on a separate key.
        """
        return key + "/expires"

    async def check(self) -> bool:
        """
        Check if storage is healthy by calling the ``get`` command
        on the key ``limiter-check``
        """
        try:
            storage = await self.get_storage()
            await storage.get(b"limiter-check")

            return True
        except:  # noqa
            return False

    async def reset(self) -> Optional[int]:
        raise NotImplementedError

    def _key_lock_name(self, key: str) -> str:
        return f"{key}/lock"

    async def __acquire_lock(self, key: str, limit_expiry: float) -> bool:
        """
        Lock acquisition on a specific memcached key. Non blocking (fail fast).
        Return True if the lock was acquired, false otherwise.
        """
        # return self.storage.add(self._key_lock_name(key), 1, expire=ceil(limit_expiry * 2))

        storage = await self.get_storage()
        added = True
        try:
            await storage.add(
                self._key_lock_name(key).encode("utf-8"),
                bytes(1),
                exptime=ceil(limit_expiry * 2),
            )
        except self.dependency.NotStoredStorageCommandError:
            added = False
        return added

    async def __release_lock(self, key: str) -> None:
        """Release the lock on a specific key. Ignore the result."""
        storage = await self.get_storage()
        await storage.delete(self._key_lock_name(key).encode("utf-8"))

    @asynccontextmanager
    async def _lock(self, key: str, limit_expiry: float) -> AsyncGenerator[None, None]:
        if not await self.__acquire_lock(key, limit_expiry):
            return  # Fail silently and exit the context if lock acquisition fails
        try:
            yield
        finally:
            await self.__release_lock(key)

    async def acquire_sliding_window_entry(
        self,
        previous_key: str,
        current_key: str,
        limit: int,
        expiry: int,
        amount: int = 1,
    ) -> bool:
        if amount > limit:
            return False
        (
            previous_count,
            previous_ttl,
            current_count,
            current_ttl,
        ) = await self.get_sliding_window(previous_key, current_key)

        if current_ttl > 0 and current_ttl < expiry:
            # Current window expired, acquire lock and shift it to the previous window
            async with self._lock(current_key, expiry):
                # Extend the current counter expiry. Recreate it if it has just expired.
                if not await self.touch(current_key, current_ttl + expiry):
                    await self.incr(current_key, current_ttl + expiry, amount=0)

                # Move the current window counter and expiry to the previous one
                await self.set(previous_key, current_count, current_ttl)
                storage = await self.get_storage()
                current_count = (
                    await storage.decrement(current_key.encode("utf-8"), current_count)
                    or 0
                )

        weighted_count = previous_count * previous_ttl / expiry + current_count
        if weighted_count + amount > limit:
            return False
        else:
            # Hit, increase the current counter.
            # If the counter doesn't exist yet, set twice the theorical expiry.
            await self.incr(current_key, expiry * 2, amount=amount)
            return True

    async def get_sliding_window(
        self, previous_key: str, current_key: str
    ) -> tuple[int, float, int, float]:
        result = await self.get_many(
            [
                previous_key,
                self._expiration_key(previous_key),
                current_key,
                self._expiration_key(current_key),
            ]
        )
        now = time.time()
        raw_previous_count, raw_previous_ttl, raw_current_count, raw_current_ttl = (
            result.get(previous_key.encode("utf-8")),
            result.get(self._expiration_key(previous_key).encode("utf-8")),
            result.get(current_key.encode("utf-8")),
            result.get(self._expiration_key(current_key).encode("utf-8")),
        )
        current_count = raw_current_count and int(raw_current_count.value) or 0
        previous_count = raw_previous_count and int(raw_previous_count.value) or 0
        current_ttl = self._ttl(
            raw_current_ttl and float(raw_current_ttl.value) or now, now
        )
        previous_ttl = self._ttl(
            raw_previous_ttl and float(raw_previous_ttl.value) or now, now
        )
        return previous_count, previous_ttl, current_count, current_ttl
