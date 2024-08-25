from contextlib import asynccontextmanager, contextmanager
from typing import ClassVar, Optional, Type, TypeVar

from meilisearch_python_sdk import AsyncClient, AsyncIndex, Client, Index
from meilisearch_python_sdk.errors import MeilisearchApiError
from meilisearch_python_sdk.models.settings import MeilisearchSettings

from .config import MeilisearchConfig

# ----------------------- #

M = TypeVar("M", bound="MeilisearchExtension")

# ....................... #


class MeilisearchExtension:

    config: ClassVar[MeilisearchConfig] = MeilisearchConfig()

    # ....................... #

    def __init_subclass__(cls: Type[M], **kwargs):
        with cls._client() as c:
            try:
                c.index(cls.config.index)

            except MeilisearchApiError:
                c.create_index(
                    cls.config.index,
                    primary_key=cls.config.primary_key,
                    settings=cls.config.settings,
                )

    # ....................... #

    @contextmanager
    @classmethod
    def _client(cls: Type[M]):
        """Get syncronous Meilisearch client"""

        url = cls.config.url()
        key = cls.config.credentials.master_key

        if key:
            api_key = key.get_secret_value()

        else:
            api_key = None

        c = Client(url=url, api_key=api_key)

        try:
            yield c

        finally:
            pass

    # ....................... #

    @asynccontextmanager
    @classmethod
    async def _aclient(cls: Type[M]):
        """Get asyncronous Meilisearch client"""

        url = cls.config.url()
        key = cls.config.credentials.master_key

        if key:
            api_key = key.get_secret_value()

        else:
            api_key = None

        c = AsyncClient(url=url, api_key=api_key)

        try:
            yield c

        finally:
            pass

    # ....................... #

    @classmethod
    def health(cls: Type[M]) -> bool:
        """Check Meilisearch health"""

        try:
            with cls._client() as c:
                h = c.health()
                status = h.status == "available"

        except Exception:
            status = False

        return status

    # ....................... #

    @classmethod
    def _index(cls: Type[M]) -> Index:
        """Get Meilisearch index"""

        with cls._client() as c:
            return c.index(cls.config.index)

    # ....................... #

    @classmethod
    async def _aindex(cls: Type[M]) -> AsyncIndex:
        """Get asyncronous Meilisearch index"""

        async with cls._aclient() as c:
            return c.index(cls.config.index)

    # ....................... #

    @classmethod
    def _update_index(cls: Type[M], settings: MeilisearchSettings):
        """Update Meilisearch index settings"""

        ix = cls._index()
        available_settings = ix.get_settings()

        if settings != available_settings:
            ix.update_settings(settings)

    # ....................... #

    @classmethod
    def search(cls: Type[M], request: dict):
        pass

    # ....................... #

    @classmethod
    async def asearch(cls: Type[M], request: dict):
        pass

    # ....................... #

    @classmethod
    def delete_documents(cls: Type[M]):
        pass

    # ....................... #

    @classmethod
    async def adelete_documents(cls: Type[M]):
        pass

    # ....................... #

    @classmethod
    def all_documents(cls: Type[M]):
        pass

    # ....................... #

    @classmethod
    async def aall_documents(cls: Type[M]):
        pass

    # ....................... #

    @classmethod
    def update_documents(cls: Type[M]):
        pass

    # ....................... #

    @classmethod
    async def aupdate_documents(cls: Type[M]):
        pass

    # ....................... #

    @classmethod
    def last_update(cls: Type[M]) -> Optional[int]:
        ix = cls._index()
        dt = ix.updated_at

        if dt:
            return int(dt.timestamp())

        return None

    # ....................... #

    @classmethod
    async def alast_update(cls: Type[M]) -> Optional[int]:
        ix = await cls._aindex()
        dt = ix.updated_at

        if dt:
            return int(dt.timestamp())

        return None
