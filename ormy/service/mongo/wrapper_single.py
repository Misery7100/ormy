from typing import ClassVar, List, Optional, Sequence, Type, TypeVar

from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)
from pymongo import InsertOne, MongoClient, UpdateMany, UpdateOne  # noqa: F401
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError, ConnectionFailure, OperationFailure

from ormy.base.abc import DocumentID, DocumentSingleABC
from ormy.base.error import BadInput, Conflict, Forbidden, InternalError, NotFound
from ormy.base.generic import TabularData

from .config import MongoConfig
from .typing import MongoRequest

# ----------------------- #

M = TypeVar("M", bound="MongoSingleBase")

# ----------------------- #


class MongoSingleBase(DocumentSingleABC):  # TODO: add docstrings
    config: ClassVar[MongoConfig] = MongoConfig()
    _registry = {MongoConfig: {}}  #! - ?????

    _static: ClassVar[Optional[MongoClient]] = None
    _astatic: ClassVar[Optional[AsyncIOMotorClient]] = None

    # ....................... #

    def __init_subclass__(cls: Type[M], **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)

        cls._register_subclass_helper(discriminator=["database", "collection"])
        cls._merge_registry()

        MongoSingleBase._registry = cls._merge_registry_helper(
            MongoSingleBase._registry,
            cls._registry,
        )

    # ....................... #

    @classmethod
    def _client(cls: Type[M]) -> MongoClient:
        """
        Get syncronous MongoDB client

        Returns:
            client (pymongo.MongoClient): Syncronous MongoDB client
        """

        health = False

        if cls._static is not None:
            try:
                check = cls._static[cls.config.ping_database].command("ping")
                health = check.get("ok", 0) == 1

            except ConnectionFailure as e:
                cls._logger.error(f"Connection failure: {e}")
                raise InternalError(e._message)

            except OperationFailure as e:
                cls._logger.error(f"Operation failure: {e}")
                raise Forbidden(e._message)

            except Exception:
                pass

        if not health or cls._static is None:
            creds = cls.config.credentials.model_dump_with_secrets()
            cls._static = MongoClient(**creds)

        return cls._static

    # ....................... #

    @classmethod
    async def _aclient(cls: Type[M]) -> AsyncIOMotorClient:
        """
        Get asyncronous MongoDB client

        Returns:
            client (motor.motor_asyncio.AsyncIOMotorClient): Asyncronous MongoDB client
        """

        health = False

        if cls._astatic is not None:
            try:
                check = await cls._astatic[cls.config.ping_database].command("ping")
                health = check.get("ok", 0) == 1

            except ConnectionFailure as e:
                cls._logger.error(f"Connection failure: {e}")
                raise InternalError(e._message)

            except OperationFailure as e:
                cls._logger.error(f"Operation failure: {e}")
                raise Forbidden(e._message)

            except Exception:
                pass

        if not health or cls._astatic is None:
            creds = cls.config.credentials.model_dump_with_secrets()
            cls._astatic = AsyncIOMotorClient(**creds)

        return cls._astatic

    # ....................... #

    @classmethod
    def _get_database(cls: Type[M]) -> Database:
        """
        Get assigned MongoDB database in syncronous mode

        Returns:
            database (pymongo.database.Database): Syncronous MongoDB database
        """

        client = cls._client()

        return client.get_database(cls.config.database)

    # ....................... #

    @classmethod
    async def _aget_database(cls: Type[M]) -> AsyncIOMotorDatabase:
        """
        Get assigned MongoDB database in asyncronous mode

        Returns:
            database (motor.motor_asyncio.AsyncIOMotorDatabase): Asyncronous MongoDB database
        """

        client = await cls._aclient()

        return client.get_database(cls.config.database)

    # ....................... #

    @classmethod
    def _get_collection(cls: Type[M]) -> Collection:
        """
        Get assigned MongoDB collection in syncronous mode

        Returns:
            collection (pymongo.collection.Collection): Syncronous MongoDB collection
        """

        database = cls._get_database()

        return database.get_collection(cls.config.collection)

    # ....................... #

    @classmethod
    async def _aget_collection(cls: Type[M]) -> AsyncIOMotorCollection:
        """
        Get assigned MongoDB collection in asyncronous mode

        Returns:
            collection (motor.motor_asyncio.AsyncIOMotorCollection): Asyncronous MongoDB collection
        """

        database = await cls._aget_database()

        return database.get_collection(cls.config.collection)

    # ....................... #

    @classmethod
    def create(cls: Type[M], data: M) -> M:
        """
        Create a new document in the collection

        Args:
            data (MongoBase): Data model to be created

        Returns:
            res (MongoBase): Created data model

        Raises:
            Conflict: Document already exists
        """

        collection = cls._get_collection()
        document = data.model_dump(mode="json")

        _id = document["id"]

        if collection.find_one({"_id": _id}):
            raise Conflict("Document already exists")

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

        Raises:
            Conflict: Document already exists
        """

        collection = await cls._aget_collection()
        document = data.model_dump(mode="json")

        _id: DocumentID = document["id"]

        if await collection.find_one({"_id": _id}):
            raise Conflict("Document already exists")

        await collection.insert_one({**document, "_id": _id})

        return data

    # ....................... #

    def save(self: M) -> M:
        """
        Save a document in the collection.
        Document will be updated if exists

        Returns:
            self (MongoBase): Saved data model
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

        Returns:
            self (MongoBase): Saved data model
        """

        collection = await self._aget_collection()
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
        """Create multiple documents in the collection"""

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
        """Create multiple documents in the collection in asyncronous mode"""

        collection = await cls._aget_collection()

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

        raise NotImplementedError

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

        raise NotImplementedError

    # ....................... #

    @classmethod
    def find(
        cls: Type[M],
        id_: Optional[DocumentID] = None,
        request: MongoRequest = {},
    ) -> M:
        """
        Find a document in the collection

        Args:
            id_ (DocumentID, optional): Document ID
            request (MongoRequest, optional): Request to find the document

        Returns:
            res (MongoBase): Found data model

        Raises:
            BadInput: Request or value is required
            NotFound: Document not found
        """

        collection = cls._get_collection()

        if not (request or id_):
            raise BadInput("Request or value is required")

        elif not request:
            request = {"_id": id_}

        document = collection.find_one(request)

        if not document:
            raise NotFound(f"Document with ID {id_} not found")

        return cls(**document)

    # ....................... #

    @classmethod
    async def afind(
        cls: Type[M],
        id_: Optional[DocumentID] = None,
        request: MongoRequest = {},
    ) -> M:
        """
        Find a document in the collection in asyncronous mode

        Args:
            id_ (DocumentID, optional): Document ID
            request (MongoRequest, optional): Request to find the document

        Returns:
            res (MongoBase): Found data model

        Raises:
            BadInput: Request or value is required
            NotFound: Document not found
        """

        collection = await cls._aget_collection()

        if not (request or id_):
            raise BadInput("Request or value is required")

        elif not request:
            request = {"_id": id_}

        document = await collection.find_one(request)

        if not document:
            raise NotFound(f"Document with ID {id_} not found")

        return cls(**document)

    # ....................... #

    @classmethod
    def count(cls: Type[M], request: MongoRequest = {}) -> int:
        """
        Count documents in the collection

        Args:
            request (MongoRequest, optional): Request to count the documents

        Returns:
            res (int): Number of documents
        """

        collection = cls._get_collection()

        return collection.count_documents(request)

    # ....................... #

    @classmethod
    async def acount(cls: Type[M], request: MongoRequest = {}) -> int:
        """
        Count documents in the collection in asyncronous mode

        Args:
            request (MongoRequest, optional): Request to count the documents

        Returns:
            res (int): Number of documents
        """

        collection = await cls._aget_collection()

        return await collection.count_documents(request)

    # ....................... #

    @classmethod
    def find_many(
        cls: Type[M],
        request: MongoRequest = {},
        limit: int = 100,
        offset: int = 0,
    ) -> List[M]:
        """
        Find multiple documents in the collection

        Args:
            request (MongoRequest, optional): Request to find the documents
            limit (int, optional): Limit the number of documents
            offset (int, optional): Offset the number of documents

        Returns:
            res (List[MongoBase]): Found data models
        """

        collection = cls._get_collection()
        documents = collection.find(request).limit(limit).skip(offset)
        clsdocs = [cls(**doc) for doc in documents]

        return clsdocs

    # ....................... #

    @classmethod
    async def afind_many(
        cls: Type[M],
        request: MongoRequest = {},
        limit: int = 100,
        offset: int = 0,
    ) -> List[M]:
        """
        Find multiple documents in the collection in asyncronous mode

        Args:
            request (MongoRequest, optional): Request to find the documents
            limit (int, optional): Limit the number of documents
            offset (int, optional): Offset the number of documents

        Returns:
            res (List[MongoBase]): Found data models
        """

        collection = await cls._aget_collection()
        cursor = collection.find(request).limit(limit).skip(offset)
        clsdocs = [cls(**doc) async for doc in cursor]

        return clsdocs

    # ....................... #

    @classmethod
    def find_all(
        cls: Type[M],
        request: MongoRequest = {},
        batch_size: int = 100,
    ) -> List[M]:
        """
        Find all documents in the collection

        Args:
            request (MongoRequest, optional): Request to find the documents
            batch_size (int, optional): Batch size

        Returns:
            res (List[MongoBase]): Found data models
        """

        cnt = cls.count(request=request)
        found: List[M] = []

        for j in range(0, cnt, batch_size):
            docs = cls.find_many(
                request,
                limit=batch_size,
                offset=j,
            )
            found.extend(docs)

        return found

    # ....................... #

    @classmethod
    async def afind_all(
        cls: Type[M],
        request: MongoRequest = {},
        batch_size: int = 100,
    ) -> List[M]:
        """
        Find all documents in the collection in asyncronous mode

        Args:
            request (MongoRequest, optional): Request to find the documents
            batch_size (int, optional): Batch size

        Returns:
            res (List[MongoBase]): Found data models
        """

        cnt = await cls.acount(request=request)
        found: List[M] = []

        for j in range(0, cnt, batch_size):
            docs = await cls.afind_many(
                request,
                limit=batch_size,
                offset=j,
            )
            found.extend(docs)

        return found

    # ....................... #

    @classmethod
    def patch(
        cls: Type[M],
        data: TabularData,
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        on: Optional[str] = None,
        left_on: Optional[str] = None,
        right_on: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> TabularData:
        """
        Extend data with documents from the collection

        Args:
            data (TabularData): Data to be extended
            include (Sequence[str], optional): Fields to include
            exclude (Sequence[str], optional): Fields to exclude
            on (str, optional): Field to join on
            left_on (str, optional): Field to join on the left
            right_on (str, optional): Field to join on the right
            prefix (str, optional): Prefix for the fields

        Returns:
            res (TabularData): Extended data

        Raises:
            BadInput: `data` is required
            BadInput: Fields `left_on` and `right_on` are required
        """

        if not data:
            raise BadInput("`data` is required")

        if on is not None:
            left_on = on
            right_on = on

        if left_on is None or right_on is None:
            raise BadInput("Fields `left_on` and `right_on` are required")

        docs = cls.find_all(request={right_on: {"$in": list(data.unique(left_on))}})
        tab_docs = TabularData(docs)

        if include is not None:
            include = list(include)
            include.append(right_on)
            include = list(set(include))

        if exclude is not None:
            exclude = [x for x in exclude if x != right_on]
            exclude = list(set(exclude))

        return data.join(
            other=tab_docs.slice(include=include, exclude=exclude),
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
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        on: Optional[str] = None,
        left_on: Optional[str] = None,
        right_on: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> TabularData:
        """
        Extend data with documents from the collection

        Args:
            data (TabularData): Data to be extended
            include (Sequence[str], optional): Fields to include
            exclude (Sequence[str], optional): Fields to exclude
            on (str, optional): Field to join on
            left_on (str, optional): Field to join on the left
            right_on (str, optional): Field to join on the right
            prefix (str, optional): Prefix for the fields

        Returns:
            res (TabularData): Extended data

        Raises:
            BadInput: `data` is required
            BadInput: Fields `left_on` and `right_on` are required
        """

        if not data:
            raise BadInput("`data` is required")

        if on is not None:
            left_on = on
            right_on = on

        if left_on is None or right_on is None:
            raise BadInput("Fields `left_on` and `right_on` are required")

        docs = await cls.afind_all(
            request={right_on: {"$in": list(data.unique(left_on))}}
        )
        tab_docs = TabularData(docs)

        if include is not None:
            include = list(include)
            include.append(right_on)
            include = list(set(include))

        if exclude is not None:
            exclude = [x for x in exclude if x != right_on]
            exclude = list(set(exclude))

        return data.join(
            other=tab_docs.slice(include=include, exclude=exclude),
            on=on,
            left_on=left_on,
            right_on=right_on,
            prefix=prefix,
        )
