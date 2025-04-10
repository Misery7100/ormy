from typing import Any, ClassVar, Literal, Optional, Self

from ormy.exceptions import BadRequest, Conflict, ModuleNotFound, NotFound

try:
    from motor.motor_asyncio import AsyncIOMotorClient
    from pymongo import InsertOne, MongoClient
    from pymongo.errors import BulkWriteError

except ImportError as e:
    raise ModuleNotFound(extra="mongo", packages=["pymongo", "motor"]) from e

from ormy.base.generic import TabularData
from ormy.document._abc import DocumentABC

from .config import MongoConfig

# ----------------------- #


class MongoBase(DocumentABC):
    """MongoDB base class"""

    config: ClassVar[MongoConfig] = MongoConfig()

    __static: ClassVar[Optional[MongoClient]] = None
    __astatic: ClassVar[Optional[AsyncIOMotorClient]] = None
    __discriminator__ = ["database", "collection"]

    # ....................... #

    @classmethod
    def _client(cls):
        """
        Get syncronous MongoDB client

        Returns:
            client (pymongo.MongoClient): Syncronous MongoDB client
        """

        if cls.__static is None:
            creds = cls.config.credentials.model_dump_with_secrets()
            cls.__static = MongoClient(**creds)

        return cls.__static

    # ....................... #

    @classmethod
    async def _aclient(cls):
        """
        Get asyncronous MongoDB client

        Returns:
            client (motor.motor_asyncio.AsyncIOMotorClient): Asyncronous MongoDB client
        """

        if cls.__astatic is None:
            creds = cls.config.credentials.model_dump_with_secrets()
            cls.__astatic = AsyncIOMotorClient(**creds)

        return cls.__astatic

    # ....................... #

    @classmethod
    def _get_database(cls):
        """
        Get assigned MongoDB database in syncronous mode

        Returns:
            database (pymongo.database.Database): Syncronous MongoDB database
        """

        client = cls._client()

        return client.get_database(cls.config.database)

    # ....................... #

    @classmethod
    async def _aget_database(cls):
        """
        Get assigned MongoDB database in asyncronous mode

        Returns:
            database (motor.motor_asyncio.AsyncIOMotorDatabase): Asyncronous MongoDB database
        """

        client = await cls._aclient()

        return client.get_database(cls.config.database)

    # ....................... #

    @classmethod
    def _get_collection(cls):
        """
        Get assigned MongoDB collection in syncronous mode

        Returns:
            collection (pymongo.collection.Collection): Syncronous MongoDB collection
        """

        database = cls._get_database()

        return database.get_collection(cls.config.collection)

    # ....................... #

    @classmethod
    async def _aget_collection(cls):
        """
        Get assigned MongoDB collection in asyncronous mode

        Returns:
            collection (motor.motor_asyncio.AsyncIOMotorCollection): Asyncronous MongoDB collection
        """

        database = await cls._aget_database()

        return database.get_collection(cls.config.collection)

    # ....................... #

    @classmethod
    def create(cls, data: Self):
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

        _id = str(document["id"])

        if collection.find_one({"_id": _id}):
            raise Conflict("Document already exists")

        collection.insert_one({**document, "_id": _id})

        return data

    # ....................... #

    @classmethod
    async def acreate(cls, data: Self):
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

        _id = str(document["id"])

        if await collection.find_one({"_id": _id}):
            raise Conflict("Document already exists")

        await collection.insert_one({**document, "_id": _id})

        return data

    # ....................... #

    def save(self: Self):
        """
        Save a document in the collection.
        Document will be updated if exists

        Returns:
            self (MongoBase): Saved data model
        """

        collection = self._get_collection()
        document = self.model_dump()

        _id = str(document["id"])

        if collection.find_one({"_id": _id}):
            collection.update_one({"_id": _id}, {"$set": document})

        else:
            collection.insert_one({**document, "_id": _id})

        return self

    # ....................... #

    async def asave(self: Self):
        """
        Save a document in the collection in asyncronous mode.
        Document will be updated if exists

        Returns:
            self (MongoBase): Saved data model
        """

        collection = await self._aget_collection()
        document = self.model_dump()

        _id = str(document["id"])

        if await collection.find_one({"_id": _id}):
            await collection.update_one({"_id": _id}, {"$set": document})

        else:
            await collection.insert_one({**document, "_id": _id})

        return self

    # ....................... #

    @classmethod
    def create_many(cls, data: list[Self], ordered: bool = False):
        """Create multiple documents in the collection"""

        collection = cls._get_collection()

        _data = [item.model_dump() for item in data]
        operations = [InsertOne({**d, "_id": d["id"]}) for d in _data]

        try:
            result = collection.bulk_write(
                requests=operations,
                ordered=ordered,
            )
            errors = result.bulk_api_result.get("writeErrors", [])
            error_idx = {err["index"]: err for err in errors}
            successful_docs = [data[i] for i in range(len(data)) if i not in error_idx]

        except BulkWriteError as e:
            errors = e.details.get("writeErrors", [])
            error_idx = {err["index"]: err for err in errors}
            successful_docs = [data[i] for i in range(len(data)) if i not in error_idx]

        return successful_docs

    # ....................... #

    @classmethod
    async def acreate_many(cls, data: list[Self], ordered: bool = False):
        """Create multiple documents in the collection in asyncronous mode"""

        collection = await cls._aget_collection()

        _data = [item.model_dump() for item in data]
        operations = [InsertOne({**d, "_id": d["id"]}) for d in _data]

        try:
            result = await collection.bulk_write(
                requests=operations,
                ordered=ordered,
            )
            errors = result.bulk_api_result.get("writeErrors", [])
            error_idx = {err["index"]: err for err in errors}
            successful_docs = [data[i] for i in range(len(data)) if i not in error_idx]

        except BulkWriteError as e:
            errors = e.details.get("writeErrors", [])
            error_idx = {err["index"]: err for err in errors}
            successful_docs = [data[i] for i in range(len(data)) if i not in error_idx]

        return successful_docs

    # ....................... #

    @classmethod
    def update_many(cls, data: list[Self], autosave: bool = True):
        raise NotImplementedError

    # ....................... #

    @classmethod
    async def aupdate_many(cls, data: list[Self], autosave: bool = True):
        raise NotImplementedError

    # ....................... #

    @classmethod
    def find(cls, id_: Optional[str] = None, request: dict[str, Any] = {}):
        """
        Find a document in the collection

        Args:
            id_ (DocumentID, optional): Document ID
            request (MongoRequest, optional): Request to find the document

        Returns:
            res (MongoBase): Found data model

        Raises:
            BadRequest: Request or value is required
            NotFound: Document not found
        """

        collection = cls._get_collection()

        if not (request or id_):
            raise BadRequest("Request or value is required")

        elif not request:
            request = {"_id": id_}

        document = collection.find_one(request)

        if not document:
            raise NotFound(f"Document with ID {id_} not found")

        return cls(**document)

    # ....................... #

    @classmethod
    async def afind(cls, id_: Optional[str] = None, request: dict[str, Any] = {}):
        """
        Find a document in the collection in asyncronous mode

        Args:
            id_ (DocumentID, optional): Document ID
            request (MongoRequest, optional): Request to find the document

        Returns:
            res (MongoBase): Found data model

        Raises:
            BadRequest: Request or value is required
            NotFound: Document not found
        """

        collection = await cls._aget_collection()

        if not (request or id_):
            raise BadRequest("Request or value is required")

        elif not request:
            request = {"_id": id_}

        document = await collection.find_one(request)

        if not document:
            raise NotFound(f"Document with ID {id_} not found")

        return cls(**document)

    # ....................... #

    @classmethod
    def count(cls, request: dict[str, Any] = {}) -> int:
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
    async def acount(cls, request: dict[str, Any] = {}) -> int:
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
    def find_many(cls, request: dict[str, Any] = {}, limit: int = 100, offset: int = 0):
        """
        Find multiple documents in the collection

        Args:
            request (MongoRequest, optional): Request to find the documents
            limit (int, optional): Limit the number of documents
            offset (int, optional): Offset the number of documents

        Returns:
            res (list[MongoBase]): Found data models
        """

        collection = cls._get_collection()
        documents = collection.find(request).limit(limit).skip(offset)
        clsdocs = [cls(**doc) for doc in documents]

        return clsdocs

    # ....................... #

    @classmethod
    async def afind_many(
        cls, request: dict[str, Any] = {}, limit: int = 100, offset: int = 0
    ):
        """
        Find multiple documents in the collection in asyncronous mode

        Args:
            request (MongoRequest, optional): Request to find the documents
            limit (int, optional): Limit the number of documents
            offset (int, optional): Offset the number of documents

        Returns:
            res (list[MongoBase]): Found data models
        """

        collection = await cls._aget_collection()
        cursor = collection.find(request).limit(limit).skip(offset)
        clsdocs = [cls(**doc) async for doc in cursor]

        return clsdocs

    # ....................... #

    @classmethod
    def find_all(cls, request: dict[str, Any] = {}, batch_size: int = 100):
        """
        Find all documents in the collection matching the request

        Args:
            request (MongoRequest, optional): Request to find the documents
            batch_size (int, optional): Batch size

        Returns:
            res (list[MongoBase]): Found data models
        """

        cnt = cls.count(request=request)
        found: list[Self] = []

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
    async def afind_all(cls, request: dict[str, Any] = {}, batch_size: int = 100):
        """
        Find all documents in the collection in asyncronous mode

        Args:
            request (MongoRequest, optional): Request to find the documents
            batch_size (int, optional): Batch size

        Returns:
            res (list[MongoBase]): Found data models
        """

        cnt = await cls.acount(request=request)
        found: list[Self] = []

        for j in range(0, cnt, batch_size):
            docs = await cls.afind_many(
                request,
                limit=batch_size,
                offset=j,
            )
            found.extend(docs)

        return found

    # ....................... #

    def kill(self):
        """
        Hard delete a document from the collection
        """

        collection = self._get_collection()
        collection.delete_one({"_id": self.id})

    # ....................... #

    async def akill(self):
        """
        Hard delete a document from the collection in asyncronous mode
        """

        collection = await self._aget_collection()
        await collection.delete_one({"_id": self.id})

    # ....................... #

    @classmethod
    def kill_many(cls, request: dict[str, Any] = {}):
        """
        Hard delete multiple documents from the collection

        Args:
            request (MongoRequest, optional): Request to delete the documents
        """

        collection = cls._get_collection()
        collection.delete_many(request)

    # ....................... #

    @classmethod
    async def akill_many(cls, request: dict[str, Any] = {}):
        """
        Hard delete multiple documents from the collection in asyncronous mode

        Args:
            request (MongoRequest, optional): Request to delete the documents
        """

        collection = await cls._aget_collection()
        await collection.delete_many(request)

    # ....................... #

    @classmethod
    def patch(
        cls,
        data: TabularData,
        include: Optional[list[str]] = None,
        on: Optional[str] = None,
        left_on: Optional[str] = None,
        right_on: Optional[str] = None,
        prefix: Optional[str] = None,
        kind: Literal["inner", "left"] = "inner",
        fill_none: Any = None,
    ):
        """
        Extend data with documents from the collection

        Args:
            data (TabularData): Data to be extended
            include (Sequence[str], optional): Fields to include
            on (str, optional): Field to join on
            left_on (str, optional): Field to join on the left
            right_on (str, optional): Field to join on the right
            prefix (str, optional): Prefix for the fields
            kind (Literal["inner", "left"], optional): Kind of join
            fill_none (Any, optional): Value to fill None

        Returns:
            res (TabularData): Extended data

        Raises:
            BadRequest: `data` is required
            BadRequest: Fields `left_on` and `right_on` are required
        """

        if not data:
            raise BadRequest("`data` is required")

        if on is not None:
            left_on = on
            right_on = on

        if left_on is None or right_on is None:
            raise BadRequest("Fields `left_on` and `right_on` are required")

        if kind == "left" and not include:  # type safe
            raise BadRequest("Fields to include are required for left join")

        docs = cls.find_all(request={right_on: {"$in": list(data.unique(left_on))}})
        tab_docs = TabularData(docs)

        if include is not None:
            include = list(include)
            include.append(right_on)
            include = list(set(include))

        if not len(tab_docs) and kind == "left":
            tab_docs = TabularData([{k: fill_none for k in include}])  # type: ignore

        # if exclude is not None:
        #     exclude = [x for x in exclude if x != right_on]
        #     exclude = list(set(exclude))

        return data.join(
            other=tab_docs.slice(include=include),
            on=on,
            left_on=left_on,
            right_on=right_on,
            prefix=prefix,
            kind=kind,
            fill_none=fill_none,
        )

    # ....................... #

    @classmethod
    async def apatch(
        cls,
        data: TabularData,
        include: Optional[list[str]] = None,
        on: Optional[str] = None,
        left_on: Optional[str] = None,
        right_on: Optional[str] = None,
        prefix: Optional[str] = None,
        kind: Literal["inner", "left"] = "inner",
        fill_none: Any = None,
    ):
        """
        Extend data with documents from the collection

        Args:
            data (TabularData): Data to be extended
            include (Sequence[str], optional): Fields to include
            on (str, optional): Field to join on
            left_on (str, optional): Field to join on the left
            right_on (str, optional): Field to join on the right
            prefix (str, optional): Prefix for the fields
            kind (Literal["inner", "left"], optional): Kind of join
            fill_none (Any, optional): Value to fill None

        Returns:
            res (TabularData): Extended data

        Raises:
            BadRequest: `data` is required
            BadRequest: Fields `left_on` and `right_on` are required
        """

        if not data:
            raise BadRequest("`data` is required")

        if on is not None:
            left_on = on
            right_on = on

        if left_on is None or right_on is None:
            raise BadRequest("Fields `left_on` and `right_on` are required")

        if kind == "left" and not include:
            raise BadRequest("Fields to include are required for left join")

        docs = await cls.afind_all(
            request={right_on: {"$in": list(data.unique(left_on))}}
        )
        tab_docs = TabularData(docs)

        if include is not None:
            include = list(include)
            include.append(right_on)
            include = list(set(include))

        if not len(tab_docs) and kind == "left":
            tab_docs = TabularData([{k: fill_none for k in include}])  # type: ignore

        # if exclude is not None:
        #     exclude = [x for x in exclude if x != right_on]
        #     exclude = list(set(exclude))

        return data.join(
            other=tab_docs.slice(include=include),
            on=on,
            left_on=left_on,
            right_on=right_on,
            prefix=prefix,
            kind=kind,
            fill_none=fill_none,
        )
