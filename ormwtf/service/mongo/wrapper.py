from abc import abstractmethod
from typing import (
    Annotated,
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
from pymongo import InsertOne, MongoClient
from pymongo.errors import BulkWriteError

from ormwtf.base import Base

from .config import MongoConfigDict

# ----------------------- #

T = TypeVar("T", bound="MongoBase")

Settings = Annotated[Dict[str, Any], "settings"]  # TODO: update
AbstractData = Annotated[Base | BaseModel | Dict[str, Any], "data"]  # TODO: update, use

# ....................... #


class MongoBase(Base):
    """
    ORM Wrapper for MongoDB based data models.

    This class provides basic CRUD operations for MongoDB collections.
    Make sure to override the `_get_settings` method to provide the
    necessary settings for the client.
    """

    config: ClassVar[MongoConfigDict] = MongoConfigDict()

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._enable_streaming()

    # ....................... #

    @staticmethod
    @abstractmethod
    def _get_settings() -> Settings:
        """
        Get settings for the client

        Returns:
            settings (Settings): Settings for the client including: host, port, username, password,
                                       replicaset, directConnection
        """
        pass

    # ....................... #

    @classmethod
    def _client(cls: Type[T]) -> MongoClient:
        """Get syncronous client"""

        settings = cls._get_settings()

        return MongoClient(**settings)

    # ....................... #

    @classmethod
    def _aclient(cls: Type[T]) -> AsyncIOMotorClient:
        """Get asyncronous client"""

        settings = cls._get_settings()

        return AsyncIOMotorClient(**settings)

    # ....................... #

    @classmethod
    def _get_database(cls: Type[T]):
        """Get assigned database in syncronous mode"""

        client = cls._client()
        database = cls.config.get("database", None)

        assert database, "Database name is required"

        return client.get_database(database)

    # ....................... #

    @classmethod
    def _aget_databsae(cls: Type[T]):
        """Get assigned database in asyncronous mode"""

        client = cls._aclient()
        database = cls.config.get("database", None)

        assert database, "Database name is required"

        return client.get_database(database)

    # ....................... #

    @classmethod
    def _get_collection(cls: Type[T]):
        """Get assigned collection in syncronous mode"""

        database = cls._get_database()
        collection = cls.config.get("collection", None)

        assert collection, "Collection name is required"

        return database.get_collection(collection)

    # ....................... #

    @classmethod
    def _aget_collection(cls: Type[T]):
        """Get assigned collection in asyncronous mode"""

        database = cls._aget_database()
        collection = cls.config.get("collection", None)

        assert collection, "Collection name is required"

        return database.get_collection(collection)

    # ....................... #

    @classmethod
    def _enable_streaming(cls: Type[T]):
        """Enable watch streams for the collection"""

        is_streaming = cls.config.get("streaming", True)

        if is_streaming:
            database = cls._get_database()
            collection = cls._get_collection()

            collection_info = database.command(
                {"listCollections": 1, "filter": {"name": collection.name}}
            )
            options = collection_info["cursor"]["firstBatch"][0].get("options", {})
            change_stream_enabled = options.get("changeStreamPreAndPostImages", {}).get(
                "enabled", False
            )

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
    def create(cls: Type[T], data: T) -> T:
        """
        Create a new document in the collection

        Args:
            data (MongoBase): Data model to be created

        Returns:
            res (MongoBase): Created data model
        """

        collection = cls.get_collection()
        document = data.model_dump()

        _id = document["id"]

        if not collection.find_one({"_id": _id}):
            collection.insert_one({**document, "_id": _id})

        else:
            raise ValueError("Document already exists")

        return cls(**document)

    # ....................... #

    @classmethod
    async def acreate(cls: Type[T], data: T) -> T:
        """
        Create a new document in the collection in asyncronous mode

        Args:
            data (MongoBase): Data model to be created

        Returns:
            res (MongoBase): Created data model
        """

        acollection = cls.aget_collection()
        document = data.model_dump()

        _id = document["id"]

        if not await acollection.find_one({"_id": _id}):
            await acollection.insert_one({**document, "_id": _id})

        else:
            raise ValueError("Document already exists")

        return cls(**document)

    # ....................... #

    def save(self: T) -> T:
        """
        Save a document in the collection.
        Document will be updated if exists
        """

        collection = self.get_collection()
        document = self.model_dump()

        _id = document["id"]

        if collection.find_one({"_id": _id}):
            collection.update_one({"_id": _id}, {"$set": document})

        else:
            collection.insert_one({**document, "_id": _id})

        return self

    # ....................... #

    async def asave(self: T) -> T:
        """
        Save a document in the collection in asyncronous mode.
        Document will be updated if exists
        """

        acollection = self.aget_collection()
        document = self.model_dump()

        _id = document["id"]

        if await acollection.find_one({"_id": _id}):
            await acollection.update_one({"_id": _id}, {"$set": document})

        else:
            await acollection.insert_one({**document, "_id": _id})

        return self

    # ....................... #

    @classmethod
    def update(
        cls: Type[T],
        id: str,
        data: Base | BaseModel,
        autosave: bool = True,
    ) -> T:
        """
        Update a document in the collection

        Args:
            id (str): Document ID
            data (Base | BaseModel): Data model to be updated
            autosave (bool): Save the document after update

        Returns:
            res (MongoBase): Updated data model
        """

        instance = cls.find(id, autoerror=True)

        if instance.is_deleted:
            keys = ["is_deleted"]

        else:
            keys = data.model_fields.keys()

        for k in keys:
            val = getattr(data, k, None)

            if val is not None and hasattr(instance, k):
                setattr(instance, k, val)

        if autosave:
            return instance.save()

        else:
            return instance

    # ....................... #

    @classmethod
    async def aupdate(
        cls: Type[T],
        id: str,
        data: Base | BaseModel,
        autosave: bool = True,
    ) -> T:
        """
        Update a document in the collection in asyncronous mode

        Args:
            id (str): Document ID
            data (Base | BaseModel): Data model to be updated
            autosave (bool): Save the document after update

        Returns:
            res (MongoBase): Updated data model
        """

        instance = await cls.afind(id, autoerror=True)

        if instance.is_deleted:
            keys = ["is_deleted"]

        else:
            keys = data.model_fields.keys()

        for k in keys:
            val = getattr(data, k, None)

            if val is not None and hasattr(instance, k):
                setattr(instance, k, val)

        if autosave:
            return await instance.asave()

        else:
            return instance

    # ....................... #

    @classmethod
    def create_many(
        cls: Type[T],
        data: Sequence[T],
        ordered: bool = False,
    ):
        collection = cls.get_collection()

        data = [item.model_dump() for item in data]
        operations = [InsertOne({**d, "_id": d["id"]}) for d in data]

        try:
            collection.bulk_write(operations, ordered=ordered)

        except BulkWriteError as e:
            return e

    # ....................... #

    @classmethod
    async def acreate_many(
        cls: Type[T],
        data: Sequence[T],
        ordered: bool = False,
    ):
        acollection = cls.aget_collection()

        data = [item.model_dump() for item in data]
        operations = [InsertOne({**d, "_id": d["id"]}) for d in data]

        try:
            await acollection.bulk_write(operations, ordered=ordered)

        except BulkWriteError as e:
            return e

    # ....................... #

    @classmethod
    def find(
        cls: Type[T],
        value: Any,
        key: str = "_id",
        autoerror: bool = False,
    ) -> Optional[T]:
        collection = cls.get_collection()
        document = collection.find_one({key: value})

        if document:
            return cls(**document)

        elif autoerror:
            raise ValueError("Not found")

    # ....................... #

    @classmethod
    async def afind(
        cls: Type[T],
        value: Any,
        key: str = "_id",
        autoerror: bool = False,
    ) -> Optional[T]:
        acollection = cls.aget_collection()
        document = await acollection.find_one({key: value})

        if document:
            return cls(**document)

        elif autoerror:
            raise ValueError("Not found")

    # ....................... #

    @classmethod
    def find_many(
        cls: Type[T],
        request: Dict[str, Dict[str, Any]] = {},
        limit: int = 100,
        offset: int = 0,
    ) -> List[T]:
        collection = cls.get_collection()
        documents = collection.find(request).limit(limit).skip(offset)

        return [cls(**doc) for doc in documents]

    # ....................... #

    @classmethod
    async def afind_many(
        cls: Type[T],
        request: Dict[str, Dict[str, Any]] = {},
        limit: int = 100,
        offset: int = 0,
    ) -> List[T]:
        acollection = cls.aget_collection()
        cursor = acollection.find(request).limit(limit).skip(offset)

        return [cls(**doc) async for doc in cursor]

    # ....................... #

    @classmethod
    def find_many_batched(
        cls: Type[T],
        request: Dict[str, Dict[str, Any]] = {},
        batch_size: int = 100,
    ) -> List[T]:
        cnt = cls.count(request)
        collection = cls.get_collection()
        found = []

        for j in range(0, cnt, batch_size):
            res = collection.find(request).skip(j).limit(batch_size)
            found.extend([cls(**doc) for doc in res])

        return found

    # ....................... #

    @classmethod
    async def afind_many_batched(
        cls: Type[T],
        request: Dict[str, Dict[str, Any]] = {},
        batch_size: int = 100,
    ) -> List[T]:
        cnt = await cls.acount(request)
        acollection = cls.aget_collection()
        found = []

        for j in range(0, cnt, batch_size):
            res = acollection.find(request).skip(j).limit(batch_size)
            found.extend([cls(**doc) async for doc in res])

        return found

    # ....................... #

    @classmethod
    def count(
        cls: Type[T],
        request: Dict[str, Dict[str, Any]] = {},
    ) -> int:
        collection = cls.get_collection()
        return collection.count_documents(request)

    # ....................... #

    @classmethod
    async def acount(
        cls: Type[T],
        request: Dict[str, Dict[str, Any]] = {},
    ) -> int:
        acollection = cls.aget_collection()
        return await acollection.count_documents(request)

    # ....................... #

    @classmethod
    def patch_records(
        cls: Type[T],
        records: List[Dict[str, Any]],
        fields: List[str],
        prefix: Optional[str] = None,
    ) -> List[Dict[str, Any]]:

        prefix = "" if prefix is None else f"{prefix}_"
        id_field = prefix + "id"

        rows = [x.model_dump() if not isinstance(x, dict) else x for x in records]
        unique_ids = list(set([x[id_field] for x in rows]))
        res = cls.find_many_batched({"_id": {"$in": unique_ids}})

        for x in rows:
            try:
                r = next((y for y in res if y.id == x[id_field]))
                for k in fields:
                    x[f"{prefix}{k}"] = getattr(r, k)

            except Exception as e:
                print(res, x, id_field)  # TODO: rewrite
                raise e

        return rows

    # ....................... #

    @classmethod  # TODO: rewrite
    def patch_schema(
        cls: Type[T],
        schema: List[Dict[str, Any]],
        fields: List[str],
        prefix: Optional[str] = None,
    ) -> List[Dict[str, Any]]:

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
