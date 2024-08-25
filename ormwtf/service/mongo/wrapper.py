from typing import Any, ClassVar, Dict, List, Optional, Type, TypeVar

from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)
from pydantic import ConfigDict
from pymongo import InsertOne, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError

from ormwtf.base.abc import DocumentOrmABC
from ormwtf.base.typing import DocumentID

from .config import MongoConfig
from .typing import MongoRequest

# ----------------------- #

T = TypeVar("T", bound="MongoBase")

# ....................... #


class MongoBase(DocumentOrmABC):  # TODO: add docstrings

    config: ClassVar[MongoConfig] = MongoConfig()
    model_config = ConfigDict(ignored_types=(MongoConfig,))

    _registry: ClassVar[Dict[str, Dict[str, Any]]] = {}

    # ....................... #

    def __init_subclass__(cls: Type[T], **kwargs):
        """Initialize subclass with extra features"""

        super().__init_subclass__(**kwargs)
        cls._enable_streaming()
        cls._register_subclass()

    # ....................... #

    @classmethod
    def _register_subclass(cls: Type[T]):
        """Register subclass in the registry"""

        db = cls.config.database
        col = cls.config.collection

        if cls.config.include_to_registry:
            cls._registry[db] = cls._registry.get(db, {})
            cls._registry[db][col] = cls

    # ....................... #

    @classmethod
    def _enable_streaming(cls: Type[T]):
        """Enable watch streams for the collection"""

        is_streaming = cls.config.streaming

        if is_streaming:
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
    def _client(cls: Type[T]) -> MongoClient:
        """Get syncronous MongoDB client"""

        creds_dict = cls.config.credentials.model_dump_with_secrets()

        return MongoClient(**creds_dict)

    # ....................... #

    @classmethod
    def _aclient(cls: Type[T]) -> AsyncIOMotorClient:
        """Get asyncronous MongoDB client"""

        creds_dict = cls.config.credentials.model_dump_with_secrets()

        return AsyncIOMotorClient(**creds_dict)

    # ....................... #

    @classmethod
    def _get_database(cls: Type[T]) -> Database:
        """Get assigned MongoDB database in syncronous mode"""

        client = cls._client()

        return client.get_database(cls.config.database)

    # ....................... #

    @classmethod
    def _aget_database(cls: Type[T]) -> AsyncIOMotorDatabase:
        """Get assigned MongoDB database in asyncronous mode"""

        client = cls._aclient()

        return client.get_database(cls.config.database)

    # ....................... #

    @classmethod
    def _get_collection(cls: Type[T]) -> Collection:
        """Get assigned MongoDB collection in syncronous mode"""

        database = cls._get_database()

        return database.get_collection(cls.config.collection)

    # ....................... #

    @classmethod
    def _aget_collection(cls: Type[T]) -> AsyncIOMotorCollection:
        """Get assigned MongoDB collection in asyncronous mode"""

        database = cls._aget_database()

        return database.get_collection(cls.config.collection)

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

        collection = cls._get_collection()
        document = data.model_dump()

        _id = document["id"]

        if collection.find_one({"_id": _id}):
            raise ValueError("Document already exists")

        collection.insert_one({**document, "_id": _id})

        return data

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

        collection = cls._aget_collection()
        document = data.model_dump()

        _id: DocumentID = document["id"]

        if await collection.find_one({"_id": _id}):
            raise ValueError("Document already exists")

        await collection.insert_one({**document, "_id": _id})

        return data

    # ....................... #

    def save(self: T) -> T:
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

    async def asave(self: T) -> T:
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

    # @classmethod
    # def update(
    #     cls: Type[T],
    #     id_: DocumentID,
    #     data: AbstractData,
    #     ignore_none: bool = True,
    #     ignore_deleted: bool = True,  # TODO: create `extra` model from this one
    #     autosave: bool = True,
    # ) -> T:
    #     """
    #     Update a document in the collection

    #     Args:
    #         id_ (str): Document ID
    #         data (AbstractData): Data model to be updated
    #         autosave (bool, optional): Save the document after update

    #     Returns:
    #         res (MongoBase): Updated data model
    #     """

    #     instance = cls.find(id_, autoerror=True)

    #     if instance.is_deleted and ignore_deleted:
    #         keys = ["is_deleted"]

    #     elif isinstance(data, dict):
    #         keys = data.keys()

    #     else:
    #         keys = data.model_fields.keys()
    #         data = data.model_dump()

    #     for k in keys:
    #         val = data.get(k, None)

    #         if not (val is None and ignore_none) and hasattr(instance, k):
    #             setattr(instance, k, val)

    #     if autosave:
    #         return instance.save()

    #     return instance

    # # ....................... #

    # @classmethod
    # async def aupdate(
    #     cls: Type[T],
    #     id_: DocumentID,
    #     data: AbstractData,
    #     ignore_none: bool = True,
    #     ignore_deleted: bool = True,  # TODO: create `extra` model from this one
    #     autosave: bool = True,
    # ) -> T:
    #     """
    #     Update a document in the collection in asyncronous mode

    #     Args:
    #         id_ (str): Document ID
    #         data (AbstractData): Data model to be updated
    #         autosave (bool, optional): Save the document after update

    #     Returns:
    #         res (MongoBase): Updated data model
    #     """

    #     instance = await cls.afind(value=id_, autoerror=True)

    #     if instance.is_deleted and ignore_deleted:
    #         keys = ["is_deleted"]

    #     elif isinstance(data, dict):
    #         keys = data.keys()

    #     else:
    #         keys = data.model_fields.keys()
    #         data = data.model_dump()

    #     for k in keys:
    #         val = data.get(k, None)

    #         if not (val is None and ignore_none) and hasattr(instance, k):
    #             setattr(instance, k, val)

    #     if autosave:
    #         return await instance.asave()

    #     return instance

    # ....................... #

    @classmethod
    def create_many(
        cls: Type[T],
        data: List[T],
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
        cls: Type[T],
        data: List[T],
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
        cls: Type[T],
        data: List[T],
        autosave: bool = True,
    ):
        """
        ...
        """

        pass

    # ....................... #

    @classmethod
    async def aupdate_many(
        cls: Type[T],
        data: List[T],
        autosave: bool = True,
    ):
        """
        ...
        """

        pass

    # ....................... #

    @classmethod
    def find(
        cls: Type[T],
        id_: Optional[DocumentID] = None,
        request: MongoRequest = {},
        bypass: bool = False,
    ) -> Optional[T]:
        """
        ...
        """

        collection = cls._get_collection()

        if not (request or id_):
            # TODO: raise a specific error (ormwtf.base.error)
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
        cls: Type[T],
        id_: Optional[DocumentID] = None,
        request: MongoRequest = {},
        bypass: bool = False,
    ) -> Optional[T]:
        """
        ...
        """

        collection = cls._aget_collection()

        if not (request or id_):
            # TODO: raise a specific error (ormwtf.base.error)
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
        cls: Type[T],
        request: MongoRequest = {},
        limit: int = 100,
        offset: int = 0,
    ) -> List[T]:
        """
        ...
        """

        collection = cls._get_collection()
        documents = collection.find(request).limit(limit).skip(offset)

        return [cls(**doc) for doc in documents]

    # ....................... #

    @classmethod
    async def afind_many(
        cls: Type[T],
        request: MongoRequest = {},
        limit: int = 100,
        offset: int = 0,
    ) -> List[T]:
        """
        ...
        """

        collection = cls._aget_collection()
        cursor = collection.find(request).limit(limit).skip(offset)

        return [cls(**doc) async for doc in cursor]

    # ....................... #

    @classmethod
    def count(
        cls: Type[T],
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
        cls: Type[T],
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
        cls: Type[T],
        request: MongoRequest = {},
        batch_size: int = 100,
    ) -> List[T]:
        """
        ...
        """

        cnt = cls.count(request=request)
        found = []

        for j in range(0, cnt, batch_size):
            docs = cls.find_many(request, limit=batch_size, offset=j)
            found.extend(docs)

        return found

    # ....................... #

    @classmethod
    async def afind_all(
        cls: Type[T],
        request: MongoRequest = {},
        batch_size: int = 100,
    ) -> List[T]:
        """
        ...
        """

        cnt = await cls.acount(request=request)
        found = []

        for j in range(0, cnt, batch_size):
            docs = await cls.afind_many(request, limit=batch_size, offset=j)
            found.extend(docs)

        return found

    # ....................... #

    #! TODO: refactor or remove

    @classmethod
    def patch_records(
        cls: Type[T],
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
        cls: Type[T],
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
