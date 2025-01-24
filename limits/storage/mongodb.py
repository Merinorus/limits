from __future__ import annotations

import datetime
import time
from abc import ABC, abstractmethod
from typing import cast

from deprecated.sphinx import versionadded, versionchanged

from limits.typing import (
    Dict,
    List,
    MongoClient,
    MongoCollection,
    MongoDatabase,
    Optional,
    Tuple,
    Type,
    Union,
)

from ..util import get_dependency
from .base import MovingWindowSupport, SlidingWindowCounterSupport, Storage


class MongoDBStorageBase(
    Storage, MovingWindowSupport, SlidingWindowCounterSupport, ABC
):
    """
    Rate limit storage with MongoDB as backend.

    Depends on :pypi:`pymongo`.
    """

    DEPENDENCIES = ["pymongo"]

    def __init__(
        self,
        uri: str,
        database_name: str = "limits",
        counter_collection_name: str = "counters",
        window_collection_name: str = "windows",
        wrap_exceptions: bool = False,
        **options: Union[int, str, bool],
    ) -> None:
        """
        :param uri: uri of the form ``mongodb://[user:password]@host:port?...``,
         This uri is passed directly to :class:`~pymongo.mongo_client.MongoClient`
        :param database_name: The database to use for storing the rate limit
         collections.
        :param counter_collection_name: The collection name to use for individual counters
         used in fixed window strategies
        :param window_collection_name: The collection name to use for moving window storage
        :param wrap_exceptions: Whether to wrap storage exceptions in
         :exc:`limits.errors.StorageError` before raising it.
        :param options: all remaining keyword arguments are passed to the
         constructor of :class:`~pymongo.mongo_client.MongoClient`
        :raise ConfigurationError: when the :pypi:`pymongo` library is not available
        """

        super().__init__(uri, wrap_exceptions=wrap_exceptions, **options)
        self._database_name = database_name
        self._collection_mapping = {
            "counters": counter_collection_name,
            "windows": window_collection_name,
        }
        self.lib = self.dependencies["pymongo"].module
        self.lib_errors, _ = get_dependency("pymongo.errors")
        self._storage_uri = uri
        self._storage_options = options
        self._storage: Optional[MongoClient] = None

    @property
    def storage(self) -> MongoClient:
        if self._storage is None:
            self._storage = self._init_mongo_client(
                self._storage_uri, **self._storage_options
            )
            self.__initialize_database()
        return self._storage

    @property
    def _database(self) -> MongoDatabase:
        return self.storage[self._database_name]

    @property
    def counters(self) -> MongoCollection:
        return self._database[self._collection_mapping["counters"]]

    @property
    def windows(self) -> MongoCollection:
        return self._database[self._collection_mapping["windows"]]

    @abstractmethod
    def _init_mongo_client(
        self, uri: Optional[str], **options: Union[int, str, bool]
    ) -> MongoClient:
        raise NotImplementedError()

    @property
    def base_exceptions(
        self,
    ) -> Union[Type[Exception], Tuple[Type[Exception], ...]]:  # pragma: no cover
        return self.lib_errors.PyMongoError  # type: ignore

    def __initialize_database(self) -> None:
        self.counters.create_index("expireAt", expireAfterSeconds=0)
        self.windows.create_index("expireAt", expireAfterSeconds=0)

    def reset(self) -> Optional[int]:
        """
        Delete all rate limit keys in the rate limit collections (counters, windows)
        """
        num_keys = self.counters.count_documents({}) + self.windows.count_documents({})
        self.counters.drop()
        self.windows.drop()

        return int(num_keys)

    def clear(self, key: str) -> None:
        """
        :param key: the key to clear rate limits for
        """
        self.counters.find_one_and_delete({"_id": key})
        self.windows.find_one_and_delete({"_id": key})

    def get_expiry(self, key: str) -> float:
        """
        :param key: the key to get the expiry for
        """
        counter = self.counters.find_one({"_id": key})
        return (
            (counter["expireAt"] if counter else datetime.datetime.now())
            .replace(tzinfo=datetime.timezone.utc)
            .timestamp()
        )

    def get(self, key: str) -> int:
        """
        :param key: the key to get the counter value for
        """
        counter = self.counters.find_one(
            {
                "_id": key,
                "expireAt": {"$gte": datetime.datetime.now(datetime.timezone.utc)},
            },
            projection=["count"],
        )

        return counter and counter["count"] or 0

    def incr(
        self, key: str, expiry: int, elastic_expiry: bool = False, amount: int = 1
    ) -> int:
        """
        increments the counter for a given rate limit key

        :param key: the key to increment
        :param expiry: amount in seconds for the key to expire in
        :param amount: the number to increment by
        """
        expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            seconds=expiry
        )

        return int(
            self.counters.find_one_and_update(
                {"_id": key},
                [
                    {
                        "$set": {
                            "count": {
                                "$cond": {
                                    "if": {"$lt": ["$expireAt", "$$NOW"]},
                                    "then": amount,
                                    "else": {"$add": ["$count", amount]},
                                }
                            },
                            "expireAt": {
                                "$cond": {
                                    "if": {"$lt": ["$expireAt", "$$NOW"]},
                                    "then": expiration,
                                    "else": (
                                        expiration if elastic_expiry else "$expireAt"
                                    ),
                                }
                            },
                        }
                    },
                ],
                upsert=True,
                projection=["count"],
                return_document=self.lib.ReturnDocument.AFTER,
            )["count"]
        )

    def check(self) -> bool:
        """
        Check if storage is healthy by calling :meth:`pymongo.mongo_client.MongoClient.server_info`
        """
        try:
            self.storage.server_info()

            return True
        except:  # noqa: E722
            return False

    def get_moving_window(self, key: str, limit: int, expiry: int) -> Tuple[float, int]:
        """
        returns the starting point and the number of entries in the moving
        window

        :param key: rate limit key
        :param expiry: expiry of entry
        :return: (start of window, number of acquired entries)
        """
        timestamp = time.time()
        result = list(
            self.windows.aggregate(
                [
                    {"$match": {"_id": key}},
                    {
                        "$project": {
                            "entries": {
                                "$filter": {
                                    "input": "$entries",
                                    "as": "entry",
                                    "cond": {"$gte": ["$$entry", timestamp - expiry]},
                                }
                            }
                        }
                    },
                    {"$unwind": "$entries"},
                    {
                        "$group": {
                            "_id": "$_id",
                            "min": {"$min": "$entries"},
                            "count": {"$sum": 1},
                        }
                    },
                ]
            )
        )

        if result:
            return result[0]["min"], result[0]["count"]

        return timestamp, 0

    def acquire_entry(self, key: str, limit: int, expiry: int, amount: int = 1) -> bool:
        """
        :param key: rate limit key to acquire an entry in
        :param limit: amount of entries allowed
        :param expiry: expiry of the entry
        :param amount: the number of entries to acquire
        """
        if amount > limit:
            return False

        timestamp = time.time()
        try:
            updates: Dict[
                str,
                Dict[str, Union[datetime.datetime, Dict[str, Union[List[float], int]]]],
            ] = {
                "$push": {
                    "entries": {
                        "$each": [timestamp] * amount,
                        "$position": 0,
                        "$slice": limit,
                    }
                },
                "$set": {
                    "expireAt": (
                        datetime.datetime.now(datetime.timezone.utc)
                        + datetime.timedelta(seconds=expiry)
                    )
                },
            }

            self.windows.update_one(
                {
                    "_id": key,
                    f"entries.{limit - amount}": {"$not": {"$gte": timestamp - expiry}},
                },
                updates,
                upsert=True,
            )

            return True
        except self.lib.errors.DuplicateKeyError:
            return False

    def get_sliding_window(
        self, key: str, expiry: Optional[int] = None
    ) -> tuple[int, float, int, float]:
        if result := list(
            self.windows.aggregate(
                [
                    {"$match": {"_id": key}},
                    {
                        "$project": {
                            # Retrieve previous window count
                            "currentCount": "$currentCount",
                            "previousCount": "$previousCount",
                            # Calculate current TTL
                            "currentTTL": {
                                "$cond": {
                                    "if": {"$lte": ["$expiresAt", "$$NOW"]},
                                    "then": 0,
                                    "else": {"$subtract": ["$expiresAt", "$$NOW"]},
                                }
                            },
                        }
                    },
                ]
            )
        ):
            window = result[0]
            return (
                window["previousCount"],
                window["currentTTL"] / 1000 - expiry,
                window["currentCount"],
                window["currentTTL"] / 1000,
            )
        return 0, 0, 0, 0

    def acquire_sliding_window_entry(
        self, key: str, limit: int, expiry: int, amount: int = 1
    ) -> bool:
        result = self.windows.find_one_and_update(
            {"_id": key},
            [
                {
                    "$set": {
                        "previousCount": {
                            "$cond": {
                                "if": {"$lte": ["$expiresAt", "$$NOW"]},
                                "then": {"$ifNull": ["$currentCount", 0]},
                                "else": {"$ifNull": ["$previousCount", 0]},
                            }
                        },
                        "currentCount": {
                            "$cond": {
                                "if": {"$lte": ["$expiresAt", "$$NOW"]},
                                "then": 0,
                                "else": {"$ifNull": ["$currentCount", 0]},
                            }
                        },
                        "expiresAt": {
                            "$cond": {
                                "if": {"$lte": ["$expiresAt", "$$NOW"]},
                                "then": {"$add": ["$$NOW", 2 * expiry * 1000]},
                                "else": "$expiresAt",
                            }
                        },
                    }
                },
                {
                    "$set": {
                        "curWeightedCount": {
                            "$add": [
                                {
                                    "$multiply": [
                                        "$previousCount",
                                        {
                                            "$divide": [
                                                {"$subtract": ["$expiresAt", "$$NOW"]},
                                                expiry * 1000,
                                            ]
                                        },
                                    ]
                                },
                                "$currentCount",
                            ]
                        }
                    }
                },
                {
                    "$set": {
                        "_acquired": {
                            "$lte": [{"$add": ["$curWeightedCount", amount]}, limit]
                        }
                    }
                },
                {"$unset": ["curWeightedCount"]},
                {
                    "$set": {
                        "currentCount": {
                            "$cond": {
                                "if": {"_acquired": True},
                                "then": {"$add": ["$currentCount", amount]},
                                "else": "$currentCount",
                            }
                        }
                    }
                },
            ],
            return_document=self.lib.ReturnDocument.AFTER,
            upsert=True,
        )

        return cast(bool, result["_acquired"])


@versionadded(version="2.1")
@versionchanged(
    version="3.14.0",
    reason="Added option to select custom collection names for windows & counters",
)
class MongoDBStorage(MongoDBStorageBase):
    STORAGE_SCHEME = ["mongodb", "mongodb+srv"]

    def _init_mongo_client(
        self, uri: Optional[str], **options: Union[int, str, bool]
    ) -> MongoClient:
        return cast(MongoClient, self.lib.MongoClient(uri, **options))
