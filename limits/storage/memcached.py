import inspect
import threading
import time
import urllib.parse
from types import ModuleType
from typing import cast

from limits.errors import ConfigurationError
from limits.storage.base import SlidingWindowCounterSupport, Storage
from limits.typing import (
    Callable,
    List,
    MemcachedClientP,
    Optional,
    P,
    R,
    Tuple,
    Type,
    Union,
)
from limits.util import get_dependency


class SlidingWindowAcquireError(RuntimeError):
    pass


class MemcachedStorage(Storage, SlidingWindowCounterSupport):
    """
    Rate limit storage with memcached as backend.

    Depends on :pypi:`pymemcache`.
    """

    STORAGE_SCHEME = ["memcached"]
    """The storage scheme for memcached"""
    DEPENDENCIES = ["pymemcache"]

    def __init__(
        self,
        uri: str,
        wrap_exceptions: bool = False,
        **options: Union[str, Callable[[], MemcachedClientP]],
    ) -> None:
        """
        :param uri: memcached location of the form
         ``memcached://host:port,host:port``,
         ``memcached:///var/tmp/path/to/sock``
        :param wrap_exceptions: Whether to wrap storage exceptions in
         :exc:`limits.errors.StorageError` before raising it.
        :param options: all remaining keyword arguments are passed
         directly to the constructor of :class:`pymemcache.client.base.PooledClient`
         or :class:`pymemcache.client.hash.HashClient` (if there are more than
         one hosts specified)
        :raise ConfigurationError: when :pypi:`pymemcache` is not available
        """
        parsed = urllib.parse.urlparse(uri)
        self.hosts = []

        for loc in parsed.netloc.strip().split(","):
            if not loc:
                continue
            host, port = loc.split(":")
            self.hosts.append((host, int(port)))
        else:
            # filesystem path to UDS

            if parsed.path and not parsed.netloc and not parsed.port:
                self.hosts = [parsed.path]  # type: ignore

        self.dependency = self.dependencies["pymemcache"].module
        self.library = str(options.pop("library", "pymemcache.client"))
        self.cluster_library = str(
            options.pop("cluster_library", "pymemcache.client.hash")
        )
        self.client_getter = cast(
            Callable[[ModuleType, List[Tuple[str, int]]], MemcachedClientP],
            options.pop("client_getter", self.get_client),
        )
        self.options = options

        if not get_dependency(self.library):
            raise ConfigurationError(
                "memcached prerequisite not available."
                " please install %s" % self.library
            )  # pragma: no cover
        self.local_storage = threading.local()
        self.local_storage.storage = None
        super().__init__(uri, wrap_exceptions=wrap_exceptions)

    @property
    def base_exceptions(
        self,
    ) -> Union[Type[Exception], Tuple[Type[Exception], ...]]:  # pragma: no cover
        return self.dependency.MemcacheError  # type: ignore[no-any-return]

    def get_client(
        self, module: ModuleType, hosts: List[Tuple[str, int]], **kwargs: str
    ) -> MemcachedClientP:
        """
        returns a memcached client.

        :param module: the memcached module
        :param hosts: list of memcached hosts
        """

        return cast(
            MemcachedClientP,
            (
                module.HashClient(hosts, **kwargs)
                if len(hosts) > 1
                else module.PooledClient(*hosts, **kwargs)
            ),
        )

    def call_memcached_func(
        self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> R:
        if "noreply" in kwargs:
            argspec = inspect.getfullargspec(func)

            if not ("noreply" in argspec.args or argspec.varkw):
                kwargs.pop("noreply")

        return func(*args, **kwargs)

    @property
    def storage(self) -> MemcachedClientP:
        """
        lazily creates a memcached client instance using a thread local
        """

        if not (hasattr(self.local_storage, "storage") and self.local_storage.storage):
            dependency = get_dependency(
                self.cluster_library if len(self.hosts) > 1 else self.library
            )[0]

            if not dependency:
                raise ConfigurationError(f"Unable to import {self.cluster_library}")
            self.local_storage.storage = self.client_getter(
                dependency, self.hosts, **self.options
            )

        return cast(MemcachedClientP, self.local_storage.storage)

    def get(self, key: str) -> int:
        """
        :param key: the key to get the counter value for
        """

        return int(self.storage.get(key) or 0)

    def clear(self, key: str) -> None:
        """
        :param key: the key to clear rate limits for
        """
        self.storage.delete(key)

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

        if not self.call_memcached_func(
            self.storage.add, key, amount, expiry, noreply=False
        ):
            value = self.storage.incr(key, amount) or amount

            if elastic_expiry:
                self.call_memcached_func(self.storage.touch, key, expiry)
                self.call_memcached_func(
                    self.storage.set,
                    key + "/expires",
                    expiry + time.time(),
                    expire=expiry,
                    noreply=False,
                )

            return value
        else:
            self.call_memcached_func(
                self.storage.set,
                key + "/expires",
                expiry + time.time(),
                expire=expiry,
                noreply=False,
            )

        return amount

    def get_expiry(self, key: str) -> float:
        """
        :param key: the key to get the expiry for
        """

        return float(self.storage.get(key + "/expires") or time.time())

    def get_ttl(self, key: str) -> float:
        """
        :param key: the key to get the TTL for
        """
        now = time.time()
        return max(0, float(self.storage.get(key + "/expires", now)) - now)

    def check(self) -> bool:
        """
        Check if storage is healthy by calling the ``get`` command
        on the key ``limiter-check``
        """
        try:
            self.call_memcached_func(self.storage.get, "limiter-check")

            return True
        except:  # noqa
            return False

    def reset(self) -> Optional[int]:
        raise NotImplementedError

    def _key_lock_name(self, key):
        return f"{key}/lock"

    def _acquire_lock(self, key, expiry) -> bool:
        """
        Lock acquisition on a specific memcached key. Non blocking (fail fast).

        Return True if the lock was acquired, false otherwise.
        """
        return self.storage.add(self._key_lock_name(key), 1, expire=expiry)

    def _release_lock(self, key):
        """Release the lock on a specific key. Ignore the result."""
        self.storage.delete(self._key_lock_name(key), noreply=True)

    def acquire_sliding_window_entry(
        self, previous_key, current_key, limit, expiry, amount=1
    ):
        current_count = None
        current_ttl = int(self.get_ttl(current_key))
        if current_ttl > 0 and current_ttl < expiry:
            try:
                if self._acquire_lock(current_key):
                    if not self.call_memcached_func(
                        self.storage.touch, current_key, current_ttl + expiry
                    ):
                        if not self.call_memcached_func(
                            self.storage.add, current_key, 0, expiry * 2
                        ):
                            if not self.call_memcached_func(
                                self.storage.touch, current_key, current_ttl + expiry
                            ):
                                raise SlidingWindowAcquireError()
                    current_count = self.get(current_key)
                    if not self.call_memcached_func(
                        self.storage.set, previous_key, current_count, current_ttl
                    ):
                        raise SlidingWindowAcquireError()
                    current_count = self.call_memcached_func(
                        self.storage.decr, current_key, current_count, 0, expiry * 2
                    )
                    self._release_lock()
            except SlidingWindowAcquireError:
                self._release_lock(current_key)
                return False
        previous_count = self.get(previous_key)
        previous_ttl = self.get_ttl(previous_key)
        if current_count is None:
            current_count = self.get(current_key)
        weighted_count = previous_count * previous_ttl / expiry + self.get(current_key)

        if weighted_count + amount > limit:
            return False

        self.incr(current_key, expiry * 2, amount=amount)
        return True

    def get_sliding_window(
        self, previous_key, current_key
    ) -> tuple[int, float, int, float]:
        return (
            self.get(previous_key),
            self.get_ttl(previous_key),
            self.get(current_key),
            self.get_ttl(current_key),
        )
