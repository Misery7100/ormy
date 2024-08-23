from contextlib import asynccontextmanager, contextmanager
from typing import ClassVar, Optional, Sequence, Type, TypeVar

from firebase_admin import firestore, firestore_async
from google.cloud.firestore_v1.async_batch import AsyncWriteBatch
from google.cloud.firestore_v1.async_collection import AsyncCollectionReference
from google.cloud.firestore_v1.async_document import AsyncDocumentReference
from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud.firestore_v1.batch import WriteBatch
from google.cloud.firestore_v1.collection import CollectionReference
from google.cloud.firestore_v1.document import DocumentReference

from ormwtf.base.abc import DocumentOrmABC
from ormwtf.base.typing import DocumentID

from .config import FirestoreConfig

# ----------------------- #

T = TypeVar("T", bound="FirestoreBase")

# ....................... #


class FirestoreBase(DocumentOrmABC):

    config: ClassVar[FirestoreConfig] = FirestoreConfig()

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)
        cls.config.credentials.validate_app()

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
        data: Sequence[T],
        autosave: bool = True,
        bypass: bool = False,
    ) -> Optional[WriteBatch]:
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

        else:
            return batch

    # ....................... #

    #! Do we need to retrieve documents?

    @classmethod
    async def acreate_many(
        cls: Type[T],
        data: Sequence[T],
        autosave: bool = True,
        bypass: bool = False,
    ) -> Optional[AsyncWriteBatch]:
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

        else:
            return batch

    # ....................... #

    @classmethod
    def update_many(
        cls: Type[T],
        data: Sequence[T],
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
        data: Sequence[T],
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
            return cls(**snapshot.to_dict())

        elif not bypass:
            raise ValueError(f"Document with ID {id_} not found")

    # ....................... #

    @classmethod
    async def afind(cls: Type[T], id_: DocumentID, bypass: bool = False) -> Optional[T]:
        """
        ...
        """

        ref = await cls._aref(id_)
        snapshot = await ref.get()

        if snapshot.exists:
            return cls(**snapshot.to_dict())

        elif not bypass:
            raise ValueError(f"Document with ID {id_} not found")

    # ....................... #

    #! TODO: Support transactions?

    @classmethod
    def find_many(
        cls: Type[T],
        filters: Optional[Sequence[FieldFilter]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[T]:
        """
        ...
        """

        collection = cls._get_collection()

        if filters:
            for f in filters:
                collection = collection.where(filter=f)

        collection = collection.limit(limit).offset(offset)
        docs = collection.get()

        return [cls(**doc.to_dict()) for doc in docs]

    # ....................... #

    #! TODO: Support transactions?

    @classmethod
    async def afind_many(
        cls: Type[T],
        filters: Optional[Sequence[FieldFilter]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> T:
        """
        ...
        """

        collection = await cls._aget_collection()

        if filters:
            for f in filters:
                collection = collection.where(filter=f)

        collection = collection.limit(limit).offset(offset)
        docs = await collection.get()

        return [cls(**doc.to_dict()) for doc in docs]

    # ....................... #

    @classmethod
    def count(
        cls: Type[T],
        filters: Optional[Sequence[FieldFilter]] = None,
    ) -> int:
        """
        ...
        """

        collection = cls._get_collection()

        if filters:
            for f in filters:
                collection = collection.where(filter=f)

        number = collection.count().get()
        number: int = number[0][0].value

        return number

    # ....................... #

    @classmethod
    async def acount(
        cls: Type[T],
        filters: Optional[Sequence[FieldFilter]] = None,
    ) -> int:
        """
        ...
        """

        collection = await cls._aget_collection()

        if filters:
            for f in filters:
                collection = collection.where(filter=f)

        number = await collection.count().get()
        number: int = number[0][0].value

        return number

    # ....................... #

    @classmethod
    def find_all(
        cls: Type[T],
        filters: Optional[Sequence[FieldFilter]] = None,
        batch_size: int = 100,
    ) -> Sequence[T]:
        """
        ...
        """

        cnt = cls.count(filters=filters)
        found = []

        for j in range(0, cnt, batch_size):
            docs = cls.find_many(filters=filters, limit=batch_size, offset=j)
            found.extend(docs)

        return found

    # ....................... #

    @classmethod
    async def afind_all(
        cls: Type[T],
        filters: Optional[Sequence[FieldFilter]] = None,
        batch_size: int = 100,
    ) -> Sequence[T]:
        """
        ...
        """

        cnt = await cls.acount(filters=filters)
        found = []

        for j in range(0, cnt, batch_size):
            docs = await cls.afind_many(filters=filters, limit=batch_size, offset=j)
            found.extend(docs)

        return found

    # ....................... #

    #! TODO: Support transactions?

    @classmethod
    def stream(
        cls: Type[T],
        filters: Optional[Sequence[FieldFilter]] = None,
    ):
        """
        ...
        """

        collection = cls._get_collection()

        if filters:
            for f in filters:
                collection = collection.where(filter=f)

        return collection.stream()

    # ....................... #

    #! TODO: Support transactions?

    @classmethod
    async def astream(
        cls: Type[T],
        filters: Optional[Sequence[FieldFilter]] = None,
    ):
        """
        ...
        """

        collection = await cls._aget_collection()

        if filters:
            for f in filters:
                collection = collection.where(filter=f)

        return collection.stream()

    # ....................... #
