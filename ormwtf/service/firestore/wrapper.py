from abc import abstractmethod
from contextlib import asynccontextmanager, contextmanager
from typing import (  # noqa: F401
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

from google.cloud.firestore_v1.async_client import AsyncClient
from google.cloud.firestore_v1.async_collection import AsyncCollectionReference
from google.cloud.firestore_v1.async_document import AsyncDocumentReference
from google.cloud.firestore_v1.client import Client
from google.cloud.firestore_v1.collection import CollectionReference
from google.cloud.firestore_v1.document import DocumentReference
from pydantic import Field

from ormwtf.base import Base
from ormwtf.base.func import hex_uuid4
from ormwtf.base.typing import DocumentID, Settings

from .config import FirestoreConfigDict

# ----------------------- #

T = TypeVar("T", bound="FirestoreBase")

# ....................... #


class FirestoreBase(Base):

    config: ClassVar[FirestoreConfigDict] = FirestoreConfigDict()

    # ....................... #

    id: str = Field(title="ID", default_factory=hex_uuid4)

    # ....................... #

    @staticmethod
    @abstractmethod
    def _get_settings() -> Settings:
        """
        Get settings for the client

        Returns:
            settings (Settings): Settings for the client including: project_id
        """
        pass

    # ....................... #

    @classmethod
    @contextmanager
    def _client(cls: Type[T]):
        settings = cls._get_settings()
        database = cls.config.get("database", None)
        project_id = settings.get("project_id", None)

        client = Client(project=project_id, database=database)

        try:
            yield client

        finally:
            client.close()

    # ....................... #

    @classmethod
    @asynccontextmanager
    def _aclient(cls: Type[T]):
        settings = cls._get_settings()
        database = cls.config.get("database", None)
        project_id = settings.get("project_id", None)

        client = AsyncClient(project=project_id, database=database)

        try:
            yield client

        finally:
            client.close()

    # ....................... #

    @classmethod
    def _get_collection(cls: Type[T]) -> Optional[CollectionReference]:
        with cls._client() as client:
            return client.collection(cls.config["collection"])

    # ....................... #

    @classmethod
    async def _aget_collection(cls: Type[T]) -> Optional[AsyncCollectionReference]:
        async with cls._aclient() as client:
            return client.collection(cls.config["collection"])

    # ....................... #

    @classmethod
    def _ref(cls: Type[T], id: DocumentID) -> Optional[DocumentReference]:
        collection = cls._get_collection()
        ref = collection.document(id)

        return ref

    # ....................... #

    @classmethod
    async def _aref(cls: Type[T], id: DocumentID) -> Optional[AsyncDocumentReference]:
        collection = await cls._aget_collection()
        ref = collection.document(id)

        return ref

    # ....................... #

    @classmethod
    def create(cls: Type[T], data: T) -> T:
        pass

    # ....................... #

    @classmethod
    async def acreate(cls: Type[T], data: T) -> T:
        pass

    # ....................... #

    def save(self: T) -> T:
        pass

    # ....................... #

    async def asave(self: T) -> T:
        pass

    # ....................... #

    @classmethod
    def update(cls: Type[T]) -> T:
        pass

    # ....................... #

    @classmethod
    async def aupdate(cls: Type[T]) -> T:
        pass

    # ....................... #

    @classmethod
    def create_many(cls: Type[T], data: Sequence[T]) -> T:
        pass

    # ....................... #

    @classmethod
    async def acreate_many(cls: Type[T], data: Sequence[T]) -> T:
        pass

    # ....................... #

    @classmethod
    def find(cls: Type[T]) -> T:
        pass

    # ....................... #

    @classmethod
    async def afind(cls: Type[T]) -> T:
        pass

    # ....................... #

    @classmethod
    def find_many(cls: Type[T]) -> T:
        pass

    # ....................... #

    @classmethod
    async def afind_many(cls: Type[T]) -> T:
        pass

    # ....................... #

    @classmethod
    def find_all(cls: Type[T]) -> T:
        pass

    # ....................... #

    @classmethod
    async def afind_all(cls: Type[T]) -> T:
        pass

    # ....................... #

    @classmethod
    def count(cls: Type[T]) -> T:
        pass

    # ....................... #

    @classmethod
    async def acount(cls: Type[T]) -> T:
        pass

    # ....................... #
