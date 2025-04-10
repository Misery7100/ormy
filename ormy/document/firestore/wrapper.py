import asyncio
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Optional, Self, cast

from ormy.exceptions import Conflict, ModuleNotFound, NotFound

try:
    import firebase_admin  # type: ignore
    import firebase_admin.firestore  # type: ignore
    import firebase_admin.firestore_async  # type: ignore
    from google.cloud import firestore_v1  # type: ignore
except ImportError as e:
    raise ModuleNotFound(extra="firestore", packages=["firebase-admin"]) from e

from ormy.document._abc import DocumentABC

from .config import FirestoreConfig

# ----------------------- #


class FirestoreBase(DocumentABC):
    """Firestore base class"""

    config: ClassVar[FirestoreConfig] = FirestoreConfig()

    __static: ClassVar[Optional[firestore_v1.Client]] = None
    __astatic: ClassVar[Optional[firestore_v1.AsyncClient]] = None
    __discriminator__ = ["database", "collection"]

    # ....................... #

    @classmethod
    def __abstract_client(cls):
        """
        Get syncronous Firestore client

        Returns:
            client: Syncronous Firestore client
        """

        project_id = cls.config.credentials.project_id
        app = cls.config.credentials.app
        database = cls.config.database

        client = firebase_admin.firestore.client(app)
        client = cast(firestore_v1.Client, client)
        client._database_string_internal = f"projects/{project_id}/databases/{database}"

        return client

    # ....................... #

    @classmethod
    def __aabstract_client(cls):
        """
        Get asyncronous Firestore client

        Returns:
            client: Asyncronous Firestore client
        """

        project_id = cls.config.credentials.project_id
        app = cls.config.credentials.app
        database = cls.config.database

        client = firebase_admin.firestore_async.client(app)
        client = cast(firestore_v1.AsyncClient, client)
        client._database_string_internal = f"projects/{project_id}/databases/{database}"

        return client

    # ....................... #

    @classmethod
    @contextmanager
    def _context_client(cls):
        """Context manager for Firestore client"""

        client = cls.__abstract_client()

        try:
            yield client

        finally:
            client.close()

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def _acontext_client(cls):
        """Async context manager for Firestore client"""

        client = cls.__aabstract_client()

        try:
            yield client

        finally:
            client.close()

    # ....................... #

    @classmethod
    def _client(cls):
        """Get Firestore client"""

        if cls.__static is None:
            cls.__static = cls.__abstract_client()

        return cls.__static

    # ....................... #

    @classmethod
    def _aclient(cls):
        """Get Firestore client"""

        if cls.__astatic is None:
            cls.__astatic = cls.__aabstract_client()

        return cls.__astatic

    # ....................... #

    # TODO: add task and atask methods

    # ....................... #

    @classmethod
    def _get_collection(cls):
        """Get Firestore collection"""

        client = cls._client()

        return client.collection(cls.config.collection)

    # ....................... #

    @classmethod
    def _aget_collection(cls):
        """Get Firestore collection"""

        client = cls._aclient()

        return client.collection(cls.config.collection)

    # ....................... #

    @classmethod
    def _ref(cls, id_: str):
        """
        Get Firestore document reference

        Args:
            id_ (str): Document ID

        Returns:
            ref: Firestore document reference
        """

        collection = cls._get_collection()
        ref = collection.document(id_)

        return ref

    # ....................... #

    @classmethod
    def _aref(cls, id_: str):
        """
        Get Firestore document reference

        Args:
            id_ (str): Document ID

        Returns:
            ref: Firestore document reference
        """

        collection = cls._aget_collection()
        ref = collection.document(id_)

        return ref

    # ....................... #

    @classmethod
    @contextmanager
    def batch(cls):
        """Get Firestore batch"""

        batch = cls._client().batch()

        try:
            yield batch

        finally:
            batch.commit()

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def abatch(cls):
        """Async context manager for Firestore batch"""

        batch = cls._aclient().batch()

        try:
            yield batch

        finally:
            await batch.commit()

    # ....................... #

    @classmethod
    @contextmanager
    def transaction(cls):
        """Context manager for Firestore transaction"""

        transaction = cls._client().transaction()

        try:
            transaction._begin()

            yield transaction

        finally:
            transaction._commit()

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def atransaction(cls):
        """Async context manager for Firestore transaction"""

        transaction = cls._aclient().transaction()

        try:
            await transaction._begin()

            yield transaction

        finally:
            await transaction._commit()

    # ....................... #

    @classmethod
    def create(cls, data: Self):
        """
        Create a new document in the collection

        Args:
            data (FirestoreBase): Data model to be created

        Returns:
            res (FirestoreBase): Created data model

        Raises:
            Conflict: Document already exists
        """

        document = data.model_dump(mode="json")
        _id = str(document["id"])
        ref = cls._ref(_id)
        snapshot = ref.get()

        if snapshot.exists:
            raise Conflict("Document already exists")

        ref.set(document)

        return data

    # ....................... #

    @classmethod
    async def acreate(cls, data: Self):
        """
        Create a new document in the collection

        Args:
            data (FirestoreBase): Data model to be created

        Returns:
            res (FirestoreBase): Created data model

        Raises:
            Conflict: Document already exists
        """

        document = data.model_dump(mode="json")
        _id = str(document["id"])
        ref = cls._aref(_id)
        snapshot = await ref.get()

        if snapshot.exists:
            raise Conflict("Document already exists")

        await ref.set(document)

        return data

    # ....................... #

    @classmethod
    def create_many(cls, data: list[Self]):
        """
        Create multiple documents in the collection

        Args:
            data (list[FirestoreBase]): Data models to be created

        Returns:
            created (list[FirestoreBase]): Created data models
        """

        error_idx: list[int] = []

        with cls.batch() as batch:
            for i, x in enumerate(data):
                doc = x.model_dump()
                _id = str(doc["id"])
                ref = cls._ref(_id)
                snapshot = ref.get()

                if not snapshot.exists:
                    batch.set(ref, doc)

                else:
                    error_idx.append(i)

        return [x for i, x in enumerate(data) if i not in error_idx]

    # ....................... #

    @classmethod
    async def acreate_many(cls, data: list[Self]):
        """
        Create multiple documents in the collection

        Args:
            data (list[FirestoreBase]): Data models to be created

        Returns:
            created (list[FirestoreBase]): Created data models
        """

        error_idx: list[int] = []

        async with cls.abatch() as batch:
            tasks = []
            dumped: list[dict[str, Any]] = []

            for x in data:
                doc = x.model_dump()
                _id = str(doc["id"])
                ref = cls._aref(_id)
                tasks.append(ref.get())
                dumped.append(doc)

            snapshots = await asyncio.gather(*tasks)

            for i, (d, s) in enumerate(zip(dumped, snapshots)):
                if not s.exists:
                    batch.set(ref, d)

                else:
                    error_idx.append(i)

        return [x for i, x in enumerate(data) if i not in error_idx]

    # ....................... #

    def save(self: Self):
        """
        Save a document in the collection.
        Document will be updated if exists

        Returns:
            self (FirestoreBase): Saved data model
        """

        document = self.model_dump()
        _id = str(document["id"])
        ref = self._ref(_id)
        ref.set(document)

        return self

    # ....................... #

    async def asave(self: Self):
        """
        Save a document in the collection

        Returns:
            self (FirestoreBase): Saved data model
        """

        document = self.model_dump()
        _id = str(document["id"])
        ref = self._aref(_id)
        await ref.set(document)

        return self

    # ....................... #

    # TODO: atomic upadate methods, transaction handling

    # ....................... #

    @classmethod
    def find(cls, id_: str):
        """
        Find a document in the collection

        Args:
            id_ (str): Document ID

        Returns:
            document (FirestoreBase): Found document

        Raises:
            NotFound: Document not found
        """

        ref = cls._ref(id_)
        snapshot = ref.get()

        if not snapshot.exists:
            raise NotFound("Document not found")

        return cls(**snapshot.to_dict())  # type: ignore

    # ....................... #

    @classmethod
    async def afind(cls, id_: str):
        """
        Find a document in the collection

        Args:
            id_ (str): Document ID

        Returns:
            document (FirestoreBase): Found document

        Raises:
            NotFound: Document not found
        """

        ref = cls._aref(id_)
        snapshot = await ref.get()

        if not snapshot.exists:
            raise NotFound("Document not found")

        return cls(**snapshot.to_dict())  # type: ignore

    # ....................... #

    @classmethod
    def find_many(
        cls,
        filters: list[firestore_v1.FieldFilter] = [],
        limit: int = 100,
        offset: int = 0,
    ):
        """
        Find multiple documents in the collection

        Args:
            filters (list[firestore_v1.FieldFilter]): Filters to apply to the query
            limit (int): Maximum number of documents to return
            offset (int): Number of documents to skip

        Returns:
            documents (list[FirestoreBase]): Found documents
        """

        collection = cls._get_collection()
        query = cast(firestore_v1.Query, collection)

        if filters:
            for f in filters:
                query = query.where(filter=f)

        query = query.limit(limit).offset(offset)
        snapshots = query.get()

        return [cls(**x.to_dict()) for x in snapshots]  # type: ignore

    # ....................... #

    @classmethod
    async def afind_many(
        cls,
        filters: list[firestore_v1.FieldFilter] = [],
        limit: int = 100,
        offset: int = 0,
    ):
        """
        Find multiple documents in the collection

        Args:
            filters (list[firestore_v1.FieldFilter]): Filters to apply to the query
            limit (int): Maximum number of documents to return
            offset (int): Number of documents to skip

        Returns:
            documents (list[FirestoreBase]): Found documents
        """

        collection = cls._aget_collection()
        query = cast(firestore_v1.AsyncQuery, collection)

        if filters:
            for f in filters:
                query = query.where(filter=f)

        query = query.limit(limit).offset(offset)
        snapshots = await query.get()

        return [cls(**x.to_dict()) for x in snapshots]  # type: ignore

    # ....................... #

    @classmethod
    def count(cls, filters: list[firestore_v1.FieldFilter] = []):
        """
        Count the number of documents in the collection

        Args:
            filters (list[firestore_v1.FieldFilter]): Filters to apply to the query

        Returns:
            count (int): Number of documents in the collection
        """

        collection = cls._get_collection()
        query = cast(firestore_v1.Query, collection)

        if filters:
            for f in filters:
                query = query.where(filter=f)

        aq = query.count()
        aq = cast(firestore_v1.aggregation.AggregationQuery, aq)  # type: ignore[assignment]
        res = aq.get()  # type: ignore[call-arg]
        number = int(res[0][0].value)  # type: ignore

        return number

    # ....................... #

    @classmethod
    async def acount(cls, filters: list[firestore_v1.FieldFilter] = []):
        """
        Count the number of documents in the collection

        Args:
            filters (list[firestore_v1.FieldFilter]): Filters to apply to the query

        Returns:
            count (int): Number of documents in the collection
        """

        collection = cls._aget_collection()
        query = cast(firestore_v1.AsyncQuery, collection)

        if filters:
            for f in filters:
                query = query.where(filter=f)

        aq = query.count()
        aq = cast(firestore_v1.async_aggregation.AsyncAggregationQuery, aq)  # type: ignore[assignment]
        res = await aq.get()  # type: ignore[call-arg]
        number = int(res[0][0].value)  # type: ignore

        return number

    # ....................... #

    @classmethod
    def find_all(
        cls,
        filters: list[firestore_v1.FieldFilter] = [],
        batch_size: int = 100,
    ):
        """
        Find all documents in the collection

        Args:
            filters (list[firestore_v1.FieldFilter]): Filters to apply to the query
            batch_size (int): Maximum number of documents to return in each batch

        Returns:
            documents (list[FirestoreBase]): Found documents
        """

        cnt = cls.count(filters=filters)
        found: list[Self] = []

        for j in range(0, cnt, batch_size):
            docs = cls.find_many(filters=filters, limit=batch_size, offset=j)
            found.extend(docs)

        return found

    # ....................... #

    @classmethod
    async def afind_all(
        cls,
        filters: list[firestore_v1.FieldFilter] = [],
        batch_size: int = 100,
    ):
        """
        Find all documents in the collection

        Args:
            filters (list[firestore_v1.FieldFilter]): Filters to apply to the query
            batch_size (int): Maximum number of documents to return in each batch

        Returns:
            documents (list[FirestoreBase]): Found documents
        """

        cnt = await cls.acount(filters=filters)
        found: list[Self] = []

        for j in range(0, cnt, batch_size):
            docs = await cls.afind_many(filters=filters, limit=batch_size, offset=j)
            found.extend(docs)

        return found

    # ....................... #

    @classmethod
    def stream(cls, filters: list[firestore_v1.FieldFilter] = []):
        """
        Stream documents in the collection

        Args:
            filters (list[firestore_v1.FieldFilter]): Filters to apply to the query

        Returns:
            stream: ...
        """

        collection = cls._get_collection()
        query = cast(firestore_v1.Query, collection)

        if filters:
            for f in filters:
                query = query.where(filter=f)

        return query.stream()

    # ....................... #

    @classmethod
    async def astream(cls, filters: list[firestore_v1.FieldFilter] = []):
        """
        Stream documents in the collection

        Args:
            filters (list[firestore_v1.FieldFilter]): Filters to apply to the query

        Returns:
            stream: ...
        """

        collection = cls._aget_collection()
        query = cast(firestore_v1.AsyncQuery, collection)

        if filters:
            for f in filters:
                query = query.where(filter=f)

        return query.stream()

    # ....................... #

    def atomic_update(self: Self, updates: dict[str, Any]):
        """
        Atomic update of the document

        Args:
            updates (dict[str, Any]): Updates to apply to the document
        """

        update_filtered = {k: v for k, v in updates.items() if hasattr(self, k)}
        ref = self._ref(self.id)
        ref.update(update_filtered)

    # ....................... #

    async def aatomic_update(self: Self, updates: dict[str, Any]):
        """
        Atomic update of the document

        Args:
            updates (dict[str, Any]): Updates to apply to the document
        """

        update_filtered = {k: v for k, v in updates.items() if hasattr(self, k)}
        ref = self._aref(self.id)
        await ref.update(update_filtered)
