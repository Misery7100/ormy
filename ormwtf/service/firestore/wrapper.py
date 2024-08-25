from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Dict, List, Optional, Type, TypeVar, cast

from firebase_admin import firestore, firestore_async  # type: ignore
from google.cloud.firestore_v1 import (
    AsyncCollectionReference,
    AsyncDocumentReference,
    AsyncQuery,
    AsyncWriteBatch,
    CollectionReference,
    DocumentReference,
    FieldFilter,
    Query,
    WriteBatch,
)
from google.cloud.firestore_v1.aggregation import AggregationQuery
from google.cloud.firestore_v1.async_aggregation import AsyncAggregationQuery
from pydantic import ConfigDict

from ormwtf.base.abc import DocumentOrmABC
from ormwtf.base.typing import DocumentID

from .config import FirestoreConfig

# ----------------------- #

T = TypeVar("T", bound="FirestoreBase")

# ....................... #


class FirestoreBase(DocumentOrmABC):  # TODO: add docstrings

    config: ClassVar[FirestoreConfig] = FirestoreConfig()
    model_config = ConfigDict(ignored_types=(FirestoreConfig,))
    _registry: ClassVar[Dict[str, Dict[str, Any]]] = {}

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)
        cls.config.credentials.validate_app()
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
    @contextmanager
    def _client(cls: Type[T]):
        """Get syncronous Firestore client"""

        project_id = cls.config.credentials.project_id
        app = cls.config.credentials.app
        database = cls.config.database

        client = firestore.client(app)
        client._database_string_internal = f"projects/{project_id}/databases/{database}"

        try:
            yield client

        finally:
            client.close()

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def _aclient(cls: Type[T]):
        """Get asyncronous Firestore client"""

        project_id = cls.config.credentials.project_id
        app = cls.config.credentials.app
        database = cls.config.database

        client = firestore_async.client(app)
        client._database_string_internal = f"projects/{project_id}/databases/{database}"

        try:
            yield client

        finally:
            client.close()

    # ....................... #

    @classmethod
    def _batch(cls: Type[T]) -> WriteBatch:
        """
        ...
        """

        with cls._client() as client:
            return client.batch()

    # ....................... #

    @classmethod
    async def _abatch(cls: Type[T]) -> AsyncWriteBatch:
        """
        ...
        """

        async with cls._aclient() as client:
            return client.batch()

    # ....................... #

    @classmethod
    def _get_collection(cls: Type[T]) -> CollectionReference:
        """Get assigned Firestore collection in syncronous mode"""

        with cls._client() as client:
            return client.collection(cls.config.collection)

    # ....................... #

    @classmethod
    async def _aget_collection(cls: Type[T]) -> AsyncCollectionReference:
        """Get assigned Firestore collection in asyncronous mode"""

        async with cls._aclient() as client:
            return client.collection(cls.config.collection)

    # ....................... #

    @classmethod
    def _ref(cls: Type[T], id_: DocumentID) -> DocumentReference:
        """
        Get a document reference from assigned collection in syncronous mode
        """

        collection = cls._get_collection()
        ref = collection.document(id_)

        return ref

    # ....................... #

    @classmethod
    async def _aref(cls: Type[T], id_: DocumentID) -> AsyncDocumentReference:
        """
        Get a document reference from assigned collection in asyncronous mode
        """

        collection = await cls._aget_collection()
        ref = collection.document(id_)

        return ref

    # ....................... #

    @classmethod
    def create(cls: Type[T], data: T) -> T:
        """
        ...
        """

        document = data.model_dump()
        _id: DocumentID = document["id"]
        ref = cls._ref(_id)
        snapshot = ref.get()

        if snapshot.exists:
            raise ValueError(f"Document with ID {_id} already exists")

        ref.set(document)

        return data

    # ....................... #

    @classmethod
    async def acreate(cls: Type[T], data: T) -> T:
        """
        ...
        """

        document = data.model_dump()
        _id: DocumentID = document["id"]
        ref = await cls._aref(_id)
        snapshot = await ref.get()

        if snapshot.exists:
            raise ValueError(f"Document with ID {_id} already exists")

        await ref.set(document)

        return data

    # ....................... #

    def save(self: T) -> T:
        """
        ...
        """

        document = self.model_dump()
        _id: DocumentID = document["id"]
        ref = self._ref(_id)
        ref.set(document)

        return self

    # ....................... #

    async def asave(self: T) -> T:
        """
        ...
        """

        document = self.model_dump()
        _id: DocumentID = document["id"]
        ref = await self._aref(_id)
        await ref.set(document)

        return self

    # ....................... #

    #! Do we need to retrieve documents?

    @classmethod
    def create_many(
        cls: Type[T],
        data: List[T],
        autosave: bool = True,
        bypass: bool = False,
    ) -> WriteBatch:
        """
        ...
        """

        batch = cls._batch()

        for x in data:
            document = x.model_dump()
            _id: DocumentID = document["id"]
            ref = cls._ref(_id)
            snapshot = ref.get()

            if snapshot.exists:
                if not bypass:
                    raise ValueError(f"Document with ID {_id} already exists")
            else:
                batch.set(ref, document)

        if autosave:
            batch.commit()

        return batch

    # ....................... #

    #! Do we need to retrieve documents?

    @classmethod
    async def acreate_many(
        cls: Type[T],
        data: List[T],
        autosave: bool = True,
        bypass: bool = False,
    ) -> AsyncWriteBatch:
        """
        ...
        """

        batch = await cls._abatch()

        for x in data:
            document = x.model_dump()
            _id: DocumentID = document["id"]
            ref = await cls._aref(_id)
            snapshot = await ref.get()

            if snapshot.exists:
                if not bypass:
                    raise ValueError(f"Document with ID {_id} already exists")

            else:
                batch.set(ref, document)

        if autosave:
            await batch.commit()

        return batch

    # ....................... #

    @classmethod
    def update_many(
        cls: Type[T],
        data: List[T],
        autosave: bool = True,
    ) -> Optional[WriteBatch]:
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
    ) -> Optional[AsyncWriteBatch]:
        """
        ...
        """

        pass

    # ....................... #

    @classmethod
    def find(cls: Type[T], id_: DocumentID, bypass: bool = False) -> Optional[T]:
        """
        ...
        """

        ref = cls._ref(id_)
        snapshot = ref.get()

        if snapshot.exists:
            return cls(**snapshot.to_dict())  # type: ignore

        elif not bypass:
            raise ValueError(f"Document with ID {id_} not found")

        return None

    # ....................... #

    @classmethod
    async def afind(cls: Type[T], id_: DocumentID, bypass: bool = False) -> Optional[T]:
        """
        ...
        """

        ref = await cls._aref(id_)
        snapshot = await ref.get()

        if snapshot.exists:
            return cls(**snapshot.to_dict())  # type: ignore

        elif not bypass:
            raise ValueError(f"Document with ID {id_} not found")

        return None

    # ....................... #

    #! TODO: Support transactions?

    @classmethod
    def find_many(
        cls: Type[T],
        filters: Optional[List[FieldFilter]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[T]:
        """
        ...
        """

        collection = cls._get_collection()
        query = cast(Query, collection)

        if filters:
            for f in filters:
                query = query.where(filter=f)

        query = query.limit(limit).offset(offset)
        docs = query.get()

        return [cls(**doc.to_dict()) for doc in docs]  # type: ignore

    # ....................... #

    #! TODO: Support transactions?

    @classmethod
    async def afind_many(
        cls: Type[T],
        filters: Optional[List[FieldFilter]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[T]:
        """
        ...
        """

        collection = await cls._aget_collection()
        query = cast(AsyncQuery, collection)

        if filters:
            for f in filters:
                query = query.where(filter=f)

        query = query.limit(limit).offset(offset)
        docs = await query.get()

        return [cls(**doc.to_dict()) for doc in docs]  # type: ignore

    # ....................... #

    @classmethod
    def count(
        cls: Type[T],
        filters: Optional[List[FieldFilter]] = None,
    ) -> int:
        """
        ...
        """

        collection = cls._get_collection()
        query = cast(Query, collection)

        if filters:
            for f in filters:
                query = query.where(filter=f)

        aq: AggregationQuery = query.count()  # type: ignore
        res = aq.get()
        number: int = res[0][0].value  # type: ignore

        return number

    # ....................... #

    @classmethod
    async def acount(
        cls: Type[T],
        filters: Optional[List[FieldFilter]] = None,
    ) -> int:
        """
        ...
        """

        collection = await cls._aget_collection()
        query = cast(AsyncQuery, collection)

        if filters:
            for f in filters:
                query = query.where(filter=f)

        aq: AsyncAggregationQuery = query.count()  # type: ignore
        res = await aq.get()
        number: int = res[0][0].value  # type: ignore

        return number

    # ....................... #

    @classmethod
    def find_all(
        cls: Type[T],
        filters: Optional[List[FieldFilter]] = None,
        batch_size: int = 100,
    ) -> List[T]:
        """
        ...
        """

        cnt = cls.count(filters=filters)
        found: List[T] = []

        for j in range(0, cnt, batch_size):
            docs = cls.find_many(filters=filters, limit=batch_size, offset=j)
            found.extend(docs)

        return found

    # ....................... #

    @classmethod
    async def afind_all(
        cls: Type[T],
        filters: Optional[List[FieldFilter]] = None,
        batch_size: int = 100,
    ) -> List[T]:
        """
        ...
        """

        cnt = await cls.acount(filters=filters)
        found: List[T] = []

        for j in range(0, cnt, batch_size):
            docs = await cls.afind_many(filters=filters, limit=batch_size, offset=j)
            found.extend(docs)

        return found

    # ....................... #

    #! TODO: Support transactions?

    @classmethod
    def stream(
        cls: Type[T],
        filters: Optional[List[FieldFilter]] = None,
    ):
        """
        ...
        """

        collection = cls._get_collection()
        query = cast(Query, collection)

        if filters:
            for f in filters:
                query = query.where(filter=f)

        return query.stream()

    # ....................... #

    #! TODO: Support transactions?

    @classmethod
    async def astream(
        cls: Type[T],
        filters: Optional[List[FieldFilter]] = None,
    ):
        """
        ...
        """

        collection = await cls._aget_collection()
        query = cast(AsyncQuery, collection)

        if filters:
            for f in filters:
                query = query.where(filter=f)

        return query.stream()

    # ....................... #
