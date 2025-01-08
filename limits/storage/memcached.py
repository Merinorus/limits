import inspect
import threading
import time
import urllib.parse
from types import ModuleType
from typing import cast

from limits.errors import ConfigurationError
from limits.storage.base import Storage
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


class MemcachedStorage(Storage):
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

    # def shift_window_if_needed(self) -> tuple[int, float, int, float]:

    # cas(current) -> value, expiry, token

    # current_expires_in
    # current_amount

    # # BEGIN ATOMIC OPERATION #1
    # current_expires_in = max(
    #     0, self.storage.get_expiry(item.key_for(*identifiers)) - time.time()
    # )
    # # print(f"Current window expires in {current_expires_in} seconds")
    # if (
    #     current_expires_in > 0
    #     and current_expires_in <= item.get_expiry()
    # ):
    #     # Current window time elapsed, move the counter to the previous window.
    #     print(
    #         "Current window time elapsed, move the counter to the previous window."
    #     )
    #     amount = self.storage.get(item.key_for(*identifiers))
    #     if item.get_expiry() <= 1:
    #         # Special case when we use a 1-second rate limiter
    #         # Since the expiry of the previous counter is set to 1 second,
    #         # It leads to higher apprxomation than expected.
    #         # So in this case only we may lower the counter
    #         # This would not happen with expiry set in milliseconds
    #         # (eg: redis supports this, memcached and this lib don't)
    #         # So we just approximate for all implementations.
    #         amount = round(amount * current_expires_in)
    #         print(f"amount rounded to {amount} before moving counter")
    #     self.storage.incr(
    #         item.previous_key_for(*identifiers),
    #         expiry=max(1, round(current_expires_in)),
    #         amount=amount,
    #     )
    #     self.storage.clear(item.key_for(*identifiers))
    # # END ATOMIC OPERATION #1
