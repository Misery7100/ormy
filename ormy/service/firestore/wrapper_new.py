from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Optional, Type, TypeVar, cast

from ormy.base.abc import DocumentSingleABC
from ormy.base.error import Conflict

from .config import FirestoreConfig

# ----------------------- #

F = TypeVar("F", bound="FirestoreBase")

# ----------------------- #


class FirestoreBase(DocumentSingleABC):
    """Firestore base class"""

    config: ClassVar[FirestoreConfig] = FirestoreConfig()

    __static: ClassVar[Optional[Any]] = None
    __astatic: ClassVar[Optional[Any]] = None

    # ....................... #

    def __init_subclass__(cls: Type[F], **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)

        cls._register_subclass_helper(discriminator=["database", "collection"])

    # ....................... #

    @classmethod
    def __abstract_client(cls: Type[F]):
        """
        Get syncronous Firestore client

        Returns:
            client: Syncronous Firestore client
        """

        from firebase_admin import firestore  # type: ignore

        project_id = cls.config.credentials.project_id
        app = cls.config.credentials.app
        database = cls.config.database

        client = firestore.client(app)
        client._database_string_internal = f"projects/{project_id}/databases/{database}"

        return client

    # ....................... #

    @classmethod
    def __aabstract_client(cls: Type[F]):
        """
        Get asyncronous Firestore client

        Returns:
            client: Asyncronous Firestore client
        """

        from firebase_admin import firestore_async  # type: ignore

        project_id = cls.config.credentials.project_id
        app = cls.config.credentials.app
        database = cls.config.database

        client = firestore_async.client(app)
        client._database_string_internal = f"projects/{project_id}/databases/{database}"

        return client

    # ....................... #

    @classmethod
    @contextmanager
    def __context_client(cls: Type[F]):
        """
        Context manager for Firestore client
        """

        client = cls.__abstract_client()

        try:
            yield client

        finally:
            client.close()

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def __acontext_client(cls: Type[F]):
        """
        Async context manager for Firestore client
        """

        client = cls.__aabstract_client()

        try:
            yield client

        finally:
            client.close()

    # ....................... #

    @classmethod
    def __client(cls: Type[F]):
        """
        Get Firestore client
        """

        from google.cloud.firestore_v1 import Client

        if cls.__static is None:
            cls.__static = cls.__abstract_client()

        cls.__static = cast(Client, cls.__static)

        return cls.__static

    # ....................... #

    @classmethod
    def __aclient(cls: Type[F]):
        """
        Get Firestore client
        """

        from google.cloud.firestore_v1 import AsyncClient

        if cls.__astatic is None:
            cls.__astatic = cls.__aabstract_client()

        cls.__astatic = cast(AsyncClient, cls.__astatic)

        return cls.__astatic

    # ....................... #

    @classmethod
    def _get_collection(cls: Type[F]):
        """
        Get Firestore collection
        """

        client = cls.__client()

        return client.collection(cls.config.collection)

    # ....................... #

    @classmethod
    def _aget_collection(cls: Type[F]):
        """
        Get Firestore collection
        """

        client = cls.__aclient()

        return client.collection(cls.config.collection)

    # ....................... #

    @classmethod
    def _ref(cls: Type[F], id_: str):
        """
        Get Firestore document reference
        """

        collection = cls._get_collection()
        ref = collection.document(id_)

        return ref

    # ....................... #

    @classmethod
    def _aref(cls: Type[F], id_: str):
        """
        Get Firestore document reference
        """

        collection = cls._aget_collection()
        ref = collection.document(id_)

        return ref

    # ....................... #

    @classmethod
    def create(cls: Type[F], data: F) -> F:
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
        _id = document["id"]
        ref = cls._ref(_id)
        snapshot = ref.get()

        if snapshot.exists:
            raise Conflict("Document already exists")

        ref.set(document)

        return data

    # ....................... #

    @classmethod
    async def acreate(cls: Type[F], data: F) -> F:
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
        _id = document["id"]
        ref = cls._ref(_id)
        snapshot = await ref.get()

        if snapshot.exists:
            raise Conflict("Document already exists")

        await ref.set(document)

        return data

    # ....................... #

    def save(self: F) -> F:
        """
        Save a document in the collection.
        Document will be updated if exists

        Returns:
            self (FirestoreBase): Saved data model
        """

        document = self.model_dump()
        _id = document["id"]
        ref = self._ref(_id)
        ref.set(document)

        return self

    # ....................... #

    async def asave(self: F) -> F:
        """
        Save a document in the collection
        """

        document = self.model_dump()
        _id = document["id"]
        ref = self._ref(_id)
        await ref.set(document)

        return self
