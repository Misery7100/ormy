from typing import Any, Dict, List, Optional, Type, TypeVar

from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)
from pymongo import InsertOne, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError

from ormy.base.abc import DocumentABC
from ormy.base.generic import TabularData
from ormy.base.typing import DocumentID
from ormy.utils.logging import LogLevel, console_logger

from .config import MongoConfig
from .typing import MongoRequest

# ----------------------- #

M = TypeVar("M", bound="MongoBase")

logger = console_logger(__name__, level=LogLevel.INFO)

# ....................... #


class MongoBase(DocumentABC):  # TODO: add docstrings

    configs = [MongoConfig()]
    _registry = {MongoConfig: {}}

    # ....................... #

    def __init_subclass__(cls: Type[M], **kwargs):
        super().__init_subclass__(**kwargs)

        cls._mongo_register_subclass()
        cls._merge_registry()
        cls._enable_streaming()

        MongoBase._registry = cls._merge_registry_helper(
            MongoBase._registry,
            cls._registry,
        )

    # ....................... #

    @classmethod
    def _mongo_register_subclass(cls: Type[M]):
        """Register subclass in the registry"""

        cfg = cls.get_config(type_=MongoConfig)
        db = cfg.database
        col = cfg.collection

        if cfg.include_to_registry and not cfg.is_default():
            logger.debug(f"Registering {cls.__name__} in {db}.{col}")
            logger.debug(f"Registry before: {cls._registry}")

            cls._registry[MongoConfig] = cls._registry.get(MongoConfig, {})
            cls._registry[MongoConfig][db] = cls._registry[MongoConfig].get(db, {})
            cls._registry[MongoConfig][db][col] = cls

            logger.debug(f"Registry after: {cls._registry}")

    # ....................... #

    @classmethod
    def _enable_streaming(cls: Type[M]):
        """Enable watch streams for the collection"""

        cfg = cls.get_config(type_=MongoConfig)
        is_streaming = cfg.streaming

        if is_streaming and not cfg.is_default():
            database = cls._get_database()
            collection = cls._get_collection()

            collection_info = database.command(
                {"listCollections": 1, "filter": {"name": collection.name}}
            )
            firstBatch = collection_info["cursor"]["firstBatch"]

            if firstBatch:
                options = firstBatch[0].get("options", {})
                change_stream_enabled = options.get(
                    "changeStreamPreAndPostImages", {}
                ).get("enabled", False)
            else:
                change_stream_enabled = False

            if not change_stream_enabled:
                collection.insert_one({"_id": f"{collection.name}_dummy"})
                database.command(
                    {
                        "collMod": collection.name,
                        "changeStreamPreAndPostImages": {"enabled": True},
                    }
                )
                collection.delete_one({"_id": f"{collection.name}_dummy"})

    # ....................... #

    @classmethod
    def _client(cls: Type[M]) -> MongoClient:
        """Get syncronous MongoDB client"""

        cfg = cls.get_config(type_=MongoConfig)
        creds_dict = cfg.credentials.model_dump_with_secrets()

        return MongoClient(**creds_dict)

    # ....................... #

    @classmethod
    def _aclient(cls: Type[M]) -> AsyncIOMotorClient:
        """Get asyncronous MongoDB client"""

        cfg = cls.get_config(type_=MongoConfig)
        creds_dict = cfg.credentials.model_dump_with_secrets()

        return AsyncIOMotorClient(**creds_dict)

    # ....................... #

    @classmethod
    def _get_database(cls: Type[M]) -> Database:
        """Get assigned MongoDB database in syncronous mode"""

        cfg = cls.get_config(type_=MongoConfig)
        client = cls._client()

        return client.get_database(cfg.database)

    # ....................... #

    @classmethod
    def _aget_database(cls: Type[M]) -> AsyncIOMotorDatabase:
        """Get assigned MongoDB database in asyncronous mode"""

        cfg = cls.get_config(type_=MongoConfig)
        client = cls._aclient()

        return client.get_database(cfg.database)

    # ....................... #

    @classmethod
    def _get_collection(cls: Type[M]) -> Collection:
        """Get assigned MongoDB collection in syncronous mode"""

        cfg = cls.get_config(type_=MongoConfig)
        database = cls._get_database()

        return database.get_collection(cfg.collection)

    # ....................... #

    @classmethod
    def _aget_collection(cls: Type[M]) -> AsyncIOMotorCollection:
        """Get assigned MongoDB collection in asyncronous mode"""

        cfg = cls.get_config(type_=MongoConfig)
        database = cls._aget_database()

        return database.get_collection(cfg.collection)

    # ....................... #

    @classmethod
    def create(cls: Type[M], data: M) -> M:
        """
        Create a new document in the collection

        Args:
            data (MongoBase): Data model to be created

        Returns:
            res (MongoBase): Created data model
        """

        collection = cls._get_collection()
        document = data.model_dump()

        _id = document["id"]

        if collection.find_one({"_id": _id}):
            raise ValueError("Document already exists")

        collection.insert_one({**document, "_id": _id})

        return data

    # ....................... #

    @classmethod
    async def acreate(cls: Type[M], data: M) -> M:
        """
        Create a new document in the collection in asyncronous mode

        Args:
            data (MongoBase): Data model to be created

        Returns:
            res (MongoBase): Created data model
        """

        collection = cls._aget_collection()
        document = data.model_dump()

        _id: DocumentID = document["id"]

        if await collection.find_one({"_id": _id}):
            raise ValueError("Document already exists")

        await collection.insert_one({**document, "_id": _id})

        return data

    # ....................... #

    def save(self: M) -> M:
        """
        Save a document in the collection.
        Document will be updated if exists
        """

        collection = self._get_collection()
        document = self.model_dump()

        _id: DocumentID = document["id"]

        if collection.find_one({"_id": _id}):
            collection.update_one({"_id": _id}, {"$set": document})

        else:
            collection.insert_one({**document, "_id": _id})

        return self

    # ....................... #

    async def asave(self: M) -> M:
        """
        Save a document in the collection in asyncronous mode.
        Document will be updated if exists
        """

        collection = self._aget_collection()
        document = self.model_dump()

        _id: DocumentID = document["id"]

        if await collection.find_one({"_id": _id}):
            await collection.update_one({"_id": _id}, {"$set": document})

        else:
            await collection.insert_one({**document, "_id": _id})

        return self

    # ....................... #

    @classmethod
    def create_many(
        cls: Type[M],
        data: List[M],
        ordered: bool = False,
    ):
        """
        ...
        """

        collection = cls._get_collection()

        _data = [item.model_dump() for item in data]
        operations = [InsertOne({**d, "_id": d["id"]}) for d in _data]

        try:
            collection.bulk_write(operations, ordered=ordered)

        # Bypass errors ????
        except BulkWriteError as e:
            return e

    # ....................... #

    @classmethod
    async def acreate_many(
        cls: Type[M],
        data: List[M],
        ordered: bool = False,
    ):
        """
        ...
        """

        collection = cls._aget_collection()

        _data = [item.model_dump() for item in data]
        operations = [InsertOne({**d, "_id": d["id"]}) for d in _data]

        try:
            await collection.bulk_write(operations, ordered=ordered)

        # Bypass errors ????
        except BulkWriteError as e:
            return e

    # ....................... #

    @classmethod
    def update_many(
        cls: Type[M],
        data: List[M],
        autosave: bool = True,
    ):
        """
        ...
        """

        pass

    # ....................... #

    @classmethod
    async def aupdate_many(
        cls: Type[M],
        data: List[M],
        autosave: bool = True,
    ):
        """
        ...
        """

        pass

    # ....................... #

    @classmethod
    def find(
        cls: Type[M],
        id_: Optional[DocumentID] = None,
        request: MongoRequest = {},
        bypass: bool = False,
    ) -> Optional[M]:
        """
        ...
        """

        collection = cls._get_collection()

        if not (request or id_):
            # TODO: raise a specific error (ormy.base.error)
            raise ValueError("Request or value is required")

        elif not request:
            request = {"_id": id_}

        document = collection.find_one(request)

        if document:
            return cls(**document)

        elif not bypass:
            raise ValueError(f"Document with ID {id_} not found")

        return document

    # ....................... #

    @classmethod
    async def afind(
        cls: Type[M],
        id_: Optional[DocumentID] = None,
        request: MongoRequest = {},
        bypass: bool = False,
    ) -> Optional[M]:
        """
        ...
        """

        collection = cls._aget_collection()

        if not (request or id_):
            # TODO: raise a specific error (ormy.base.error)
            raise ValueError("Request or value is required")

        elif not request:
            request = {"_id": id_}

        document = await collection.find_one(request)

        if document:
            return cls(**document)

        elif not bypass:
            raise ValueError(f"Document with ID {id_} not found")

        return document

    # ....................... #

    @classmethod
    def find_many(
        cls: Type[M],
        request: MongoRequest = {},
        limit: int = 100,
        offset: int = 0,
        tabular: bool = True,
    ) -> TabularData | List[M]:
        """
        ...
        """

        collection = cls._get_collection()
        documents = collection.find(request).limit(limit).skip(offset)

        if tabular:
            return TabularData([doc for doc in documents])

        return [cls(**doc) for doc in documents]

    # ....................... #

    @classmethod
    async def afind_many(
        cls: Type[M],
        request: MongoRequest = {},
        limit: int = 100,
        offset: int = 0,
        tabular: bool = True,
    ) -> TabularData | List[M]:
        """
        ...
        """

        collection = cls._aget_collection()
        cursor = collection.find(request).limit(limit).skip(offset)

        if tabular:
            return TabularData([doc async for doc in cursor])

        return [cls(**doc) async for doc in cursor]

    # ....................... #

    @classmethod
    def count(
        cls: Type[M],
        request: MongoRequest = {},
    ) -> int:
        """
        ...
        """

        collection = cls._get_collection()

        return collection.count_documents(request)

    # ....................... #

    @classmethod
    async def acount(
        cls: Type[M],
        request: MongoRequest = {},
    ) -> int:
        """
        ...
        """

        collection = cls._aget_collection()

        return await collection.count_documents(request)

    # ....................... #

    @classmethod
    def find_all(
        cls: Type[M],
        request: MongoRequest = {},
        batch_size: int = 100,
        tabular: bool = False,
    ) -> TabularData | List[M]:
        """
        ...
        """

        cnt = cls.count(request=request)
        found: TabularData | List[M] = []

        for j in range(0, cnt, batch_size):
            docs = cls.find_many(
                request,
                limit=batch_size,
                offset=j,
                tabular=tabular,
            )
            found.extend(docs)

        return TabularData(found) if tabular else found

    # ....................... #

    @classmethod
    async def afind_all(
        cls: Type[M],
        request: MongoRequest = {},
        batch_size: int = 100,
        tabular: bool = False,
    ) -> TabularData | List[M]:
        """
        ...
        """

        cnt = await cls.acount(request=request)
        found: TabularData | List[M] = []

        for j in range(0, cnt, batch_size):
            docs = await cls.afind_many(
                request,
                limit=batch_size,
                offset=j,
                tabular=tabular,
            )
            found.extend(docs)

        return TabularData(found) if tabular else found

    # ....................... #

    @classmethod
    def patch(
        cls: Type[M],
        data: TabularData,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        on: Optional[str] = None,
        left_on: Optional[str] = None,
        right_on: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> TabularData:
        """
        ...
        """

        if on is not None:
            left_on = on
            right_on = on

        assert left_on is not None and right_on is not None, "Fields are required"

        find = cls.find_all(
            request={right_on: {"$in": list(data.unique(left_on))}},
            tabular=True,
        )

        return data.join(
            find.slice(include, exclude),  # type: ignore
            on=on,
            left_on=left_on,
            right_on=right_on,
            prefix=prefix,
        )

    # ....................... #

    @classmethod
    async def apatch(
        cls: Type[M],
        data: TabularData,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        on: Optional[str] = None,
        left_on: Optional[str] = None,
        right_on: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> TabularData:
        """
        ...
        """

        if on is not None:
            left_on = on
            right_on = on

        assert left_on is not None and right_on is not None, "Fields are required"

        find = await cls.afind_all(
            request={right_on: {"$in": list(data.unique(left_on))}},
            tabular=True,
        )

        return data.join(
            find.slice(include, exclude),  # type: ignore
            on=on,
            left_on=left_on,
            right_on=right_on,
            prefix=prefix,
        )

    # ....................... #

    #! TODO: refactor or remove

    @classmethod
    def patch_records(
        cls: Type[M],
        records: List[Dict[str, Any]],
        fields: List[str],
        prefix: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        ...
        """

        prefix = "" if prefix is None else f"{prefix}_"
        id_field = prefix + "id"

        unique_ids = list(set([x[id_field] for x in records]))
        res = cls.find_all({"_id": {"$in": unique_ids}})

        for x in records:
            try:
                r = next((y for y in res if y.id == x[id_field]))
                for k in fields:
                    x[f"{prefix}{k}"] = getattr(r, k)

            except Exception as e:
                print(res, x, id_field)  # TODO: rewrite
                raise e

        return records

    # ....................... #

    #! TODO: refactor or remove

    @classmethod  # TODO: rewrite
    def patch_schema(
        cls: Type[M],
        schema: List[Dict[str, Any]],
        fields: List[str],
        prefix: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        ...
        """

        model_schema = cls.model_json_schema()
        upd = []
        prefix = "" if prefix is None else f"{prefix}_"

        for k, v in model_schema["properties"].items():
            if k in fields:
                upd.append(
                    {
                        "key": f"{prefix}{k}",
                        "title": v["title"],
                        "type": v.get("type", "string"),
                    }
                )

        return schema + upd
