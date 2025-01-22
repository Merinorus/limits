import inspect
import threading
import time
import urllib.parse
from math import ceil
from types import ModuleType
from typing import Any, Iterable, cast

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
                "memcached prerequisite not available. please install %s" % self.library
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
        return int(self.storage.get(key, "0"))

    def get_many(self, keys: Iterable[str]) -> dict[str, Any]:  # type:ignore[misc]
        """
        Return multiple counters at once
        :param key: the key to get the counter value for
        """
        return self.storage.get_many(keys)

    def clear(self, key: str) -> None:
        """
        :param key: the key to clear rate limits for
        """
        self.storage.delete(key)

    def set(self, key: str, value: int, expiry: float) -> None:
        """
        set the counter for a given rate limit key to the specificied amount

        :param key: the key to set
        :param expiry: amount in seconds for the key to expire in
         window every hit.
        :param amount: the number to set to
        """
        # TODO: why setting noreply=False increase IOPS?

        self.call_memcached_func(
            self.storage.set, key, value, ceil(expiry), noreply=False
        )
        self.call_memcached_func(
            self.storage.set,
            self._expiration_key(key),
            expiry + time.time(),
            expire=ceil(expiry),
            noreply=False,
        )

    def touch(self, key: str, expiry: float) -> bool:
        """
        set the a new expiry for a given counter

        :param key: the key to set
        :param expiry: the new amount in seconds for the key to expire
        """
        if self.call_memcached_func(
            self.storage.touch, key, ceil(expiry), noreply=False
        ):
            return self.call_memcached_func(
                self.storage.set,
                self._expiration_key(key),
                expiry + time.time(),
                expire=ceil(expiry),
                noreply=False,
            )
        return False

    def incr(
        self,
        key: str,
        expiry: float,
        elastic_expiry: bool = False,
        amount: int = 1,
    ) -> int:
        """
        increments the counter for a given rate limit key

        :param key: the key to increment
        :param expiry: amount in seconds for the key to expire in
        :param elastic_expiry: whether to keep extending the rate limit
         window every hit.
        :param amount: the number to increment by
        """
        value = self.call_memcached_func(self.storage.incr, key, amount, noreply=False)
        if value is not None:
            if elastic_expiry:
                self.call_memcached_func(self.storage.touch, key, ceil(expiry))
                self.call_memcached_func(
                    self.storage.set,
                    self._expiration_key(key),
                    expiry + time.time(),
                    expire=ceil(expiry),
                    noreply=False,
                )

            return value
        else:
            if not self.call_memcached_func(
                self.storage.add, key, amount, ceil(expiry), noreply=False
            ):
                value = self.storage.incr(key, amount) or amount

                if elastic_expiry:
                    self.call_memcached_func(self.storage.touch, key, ceil(expiry))
                    self.call_memcached_func(
                        self.storage.set,
                        self._expiration_key(key),
                        expiry + time.time(),
                        expire=ceil(expiry),
                        noreply=False,
                    )

                return value
            else:
                self.call_memcached_func(
                    self.storage.set,
                    self._expiration_key(key),
                    expiry + time.time(),
                    expire=ceil(expiry),
                    noreply=False,
                )

            return amount

    def get_expiry(self, key: str) -> float:
        """
        :param key: the key to get the expiry for
        """

        return float(self.storage.get(self._expiration_key(key)) or time.time())

    def _ttl(self, expiry: Optional[Union[float, bytes]]) -> float:
        return max(0, float(expiry or 0) - time.time())

    def get_ttl(self, key: str) -> float:
        """
        :param key: the key to get the TTL for
        """
        now = time.time()
        return max(0, float(self.storage.get(self._expiration_key(key)) or now) - now)

    def _expiration_key(self, key: str) -> str:
        """
        Return the expiration key for the given counter key.

        Memcached doesn't natively return the expiration time or TTL for a given key,
        so we implement the expiration time on a separate key.
        """
        return key + "/expires"

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

    def _current_window_key(
        self, key: str, expiry: int, now: Optional[float] = None
    ) -> str:
        """
        Return the current window's storage key (Sliding window strategy)

        Contrary to other strategies that have one key per rate limit item,
        this strategy has two keys per rate limit item than must be on the same machine.
        To keep the current key and the previous key on the same Redis cluster node,
        curvy braces are added.

        Eg: "{constructed_key}"
        """
        if now is None:
            now = time.time()
        return f"{key}/{int(now / expiry)}"

    def _previous_window_key(
        self, key: str, expiry: int, now: Optional[float] = None
    ) -> str:
        """
        Return the previous window's storage key (Sliding window strategy).

        Curvy braces are added on the common pattern with the current window's key,
        so the current and the previous key are stored on the same Redis cluster node.

        Eg: "{constructed_key}/-1"
        """
        if now is None:
            now = time.time()
        return f"{key}/{int((now - expiry) / expiry)}"

    def acquire_sliding_window_entry(
        self,
        key: str,
        limit: int,
        expiry: int,
        amount: int = 1,
    ) -> bool:
        if amount > limit:
            return False
        now = time.time()
        current_key = self._current_window_key(key, expiry, now)
        # previous_key = self._previous_window_key(key, expiry, now)
        previous_count, previous_ttl, current_count, _ = self._get_sliding_window_info(
            key, expiry, get_current_ttl=False
        )

        weighted_count = previous_count * previous_ttl / expiry + current_count
        if weighted_count + amount > limit:
            return False
        else:
            # Hit, increase the current counter.
            # If the counter doesn't exist yet, set twice the theorical expiry.
            self.incr(current_key, expiry * 2, amount=amount)
            return True

    def get_sliding_window(
        self, key: str, expiry: Optional[int] = None
    ) -> tuple[int, float, int, float]:
        return self._get_sliding_window_info(key, expiry)

    def _get_sliding_window_info(
        self, key: str, expiry: Optional[int] = None, get_current_ttl: bool = True
    ) -> tuple[int, float, int, float]:
        if expiry is None:
            raise ValueError("the expiry value is needed for this storage.")

        now = time.time()
        current_key = self._current_window_key(key, expiry, now)
        previous_key = self._previous_window_key(key, expiry, now)
        if get_current_ttl:
            result = self.get_many(
                [
                    previous_key,
                    self._expiration_key(previous_key),
                    current_key,
                    self._expiration_key(current_key),
                ]
            )
        else:
            result = self.get_many(
                [
                    previous_key,
                    self._expiration_key(previous_key),
                    current_key,
                ]
            )
        previous_count, previous_ttl, current_count, current_ttl = (
            int(result.get(previous_key, 0)),
            self._ttl(result.get(self._expiration_key(previous_key))),
            int(result.get(current_key, 0)),
            self._ttl(result.get(self._expiration_key(current_key))),
        )
        return previous_count, previous_ttl, current_count, current_ttl
