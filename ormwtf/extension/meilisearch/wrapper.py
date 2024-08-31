from contextlib import asynccontextmanager, contextmanager
from typing import Any, Dict, List, Optional, Type, TypeVar

from meilisearch_python_sdk import AsyncClient, AsyncIndex, Client, Index
from meilisearch_python_sdk.errors import MeilisearchApiError
from meilisearch_python_sdk.models.search import SearchResults
from meilisearch_python_sdk.models.settings import MeilisearchSettings
from meilisearch_python_sdk.types import JsonDict

from ormwtf.base.abc import AbstractABC
from ormwtf.utils.logging import LogLevel, console_logger

from .config import MeilisearchConfig
from .schema import SearchRequest, SearchResponse

# ----------------------- #

M = TypeVar("M", bound="MeilisearchExtension")
logger = console_logger(__name__, level=LogLevel.INFO)

# ....................... #


class MeilisearchExtension(AbstractABC):

    configs = [MeilisearchConfig()]
    _config_type = MeilisearchConfig
    _registry = {MeilisearchConfig: {}}

    # ....................... #

    def __init_subclass__(cls: Type[M], **kwargs):
        super().__init_subclass__(**kwargs)

        cls._meili_register_subclass()
        cls._merge_registry()
        cls._meili_safe_create_or_update()

        MeilisearchExtension._registry = cls._merge_registry_helper(
            MeilisearchExtension._registry,
            cls._registry,
        )

    # ....................... #

    @classmethod
    def _meili_register_subclass(cls: Type[M]):
        """Register subclass in the registry"""

        cfg = cls.get_config(type_=MeilisearchConfig)
        ix = cfg.index

        if cfg.include_to_registry and not cfg.is_default():
            logger.debug(f"Registering {cls.__name__} in {ix}")
            logger.debug(f"Registry before: {cls._registry}")

            cls._registry[MeilisearchConfig] = cls._registry.get(MeilisearchConfig, {})
            cls._registry[MeilisearchConfig][ix] = cls

            logger.debug(f"Registry after: {cls._registry}")

    # ....................... #

    @classmethod
    def _meili_safe_create_or_update(cls: Type[M]):
        cfg = cls.get_config(type_=MeilisearchConfig)

        if not cfg.is_default():
            with cls._meili_client() as c:
                try:
                    ix = c.get_index(cfg.index)
                    logger.debug(f"Index `{cfg.index}` already exists")

                    if ix.get_settings() != cfg.settings:
                        cls._meili_update_index(cfg.settings)
                        logger.debug(f"Update of index `{cfg.index}` is started")

                except MeilisearchApiError:
                    c.create_index(
                        cfg.index,
                        primary_key=cfg.primary_key,
                        settings=cfg.settings,
                    )
                    logger.debug(f"Index `{cfg.index}` is created")

    # ....................... #

    @classmethod
    @contextmanager
    def _meili_client(cls: Type[M]):
        """Get syncronous Meilisearch client"""

        cfg = cls.get_config(type_=MeilisearchConfig)
        url = cfg.url()
        key = cfg.credentials.master_key

        if key:
            api_key = key.get_secret_value()

        else:
            api_key = None

        c = Client(
            url=url,
            api_key=api_key,
            custom_headers={"Content-Type": "application/json"},
        )

        try:
            yield c

        finally:
            pass

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def _ameili_client(cls: Type[M]):
        """Get asyncronous Meilisearch client"""

        cfg = cls.get_config(type_=MeilisearchConfig)
        url = cfg.url()
        key = cfg.credentials.master_key

        if key:
            api_key = key.get_secret_value()

        else:
            api_key = None

        c = AsyncClient(
            url=url,
            api_key=api_key,
            custom_headers={"Content-Type": "application/json"},
        )

        try:
            yield c

        finally:
            pass

    # ....................... #

    @classmethod
    def meili_health(cls: Type[M]) -> bool:
        """Check Meilisearch health"""

        try:
            with cls._meili_client() as c:
                h = c.health()
                status = h.status == "available"

        except Exception:
            status = False

        return status

    # ....................... #

    @classmethod
    def _meili_index(cls: Type[M]) -> Index:
        """Get associated Meilisearch index"""

        cfg = cls.get_config(type_=MeilisearchConfig)

        with cls._meili_client() as c:
            return c.get_index(cfg.index)

    # ....................... #

    @classmethod
    async def _ameili_index(cls: Type[M]) -> AsyncIndex:
        """Get associated Meilisearch index in asyncronous mode"""

        cfg = cls.get_config(type_=MeilisearchConfig)

        async with cls._ameili_client() as c:
            return await c.get_index(cfg.index)

    # ....................... #

    @classmethod
    def _meili_update_index(cls: Type[M], settings: MeilisearchSettings):
        """Update Meilisearch index settings"""

        ix = cls._meili_index()
        available_settings = ix.get_settings()

        if settings != available_settings:
            ix.update_settings(settings)

    # ....................... #

    @classmethod
    def _meili_prepare_request(
        cls: Type[M],
        request: SearchRequest,
        page: int = 1,
        size: int = 20,
    ):
        """Prepare search request"""

        cfg = cls.get_config(type_=MeilisearchConfig)
        sortable = cfg.settings.sortable_attributes

        if sortable is None:
            sortable = []

        if request.sort and request.sort in sortable:
            sort = [f"{request.sort}:{request.order.value}"]

        else:
            sort = None

        return {
            "query": request.query,
            "hits_per_page": size,
            "page": page,
            "sort": sort,
            "filter": request.filters,
        }

    # ....................... #

    @staticmethod
    def _meili_prepare_response(res: SearchResults) -> SearchResponse:
        return SearchResponse.from_search_results(res)

    # ....................... #

    @classmethod
    def meili_search(
        cls: Type[M],
        request: SearchRequest,
        page: int = 1,
        size: int = 20,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> SearchResponse:
        """
        ...
        """

        if exclude is not None and include is None:
            include = [x for x in cls.model_fields.keys() if x not in exclude]

        ix = cls._meili_index()
        req = cls._meili_prepare_request(request, page, size)
        res = ix.search(
            attributes_to_retrieve=include,
            **req,
        )

        return cls._meili_prepare_response(res)

    # ....................... #

    @classmethod
    async def ameili_search(
        cls: Type[M],
        request: SearchRequest,
        page: int = 1,
        size: int = 20,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> SearchResponse:
        """
        ...
        """
        if exclude is not None and include is None:
            include = [x for x in cls.model_fields.keys() if x not in exclude]

        ix = await cls._ameili_index()
        req = cls._meili_prepare_request(request, page, size)
        res = await ix.search(
            attributes_to_retrieve=include,
            **req,
        )

        return cls._meili_prepare_response(res)

    # ....................... #

    @classmethod
    def meili_delete_documents(cls: Type[M], ids: List[str]):
        ix = cls._meili_index()
        ix.delete_documents(ids)

    # ....................... #

    @classmethod
    async def ameili_delete_documents(cls: Type[M], ids: List[str]):
        ix = await cls._ameili_index()
        await ix.delete_documents(ids)

    # ....................... #

    @classmethod
    def _meili_all_documents(cls: Type[M]):
        ix = cls._meili_index()
        res: List[JsonDict] = []
        offset = 0

        while docs := ix.get_documents(offset=offset, limit=1000):
            res.extend(docs.results)
            offset += 1000

        return res

    # ....................... #

    @classmethod
    async def _ameili_all_documents(cls: Type[M]):
        ix = await cls._ameili_index()
        res: List[JsonDict] = []
        offset = 0

        while docs := await ix.get_documents(offset=offset, limit=1000):
            res.extend(docs.results)
            offset += 1000

        return res

    # ....................... #

    @classmethod
    def meili_update_documents(cls: Type[M], docs: List[Dict[str, Any]]):
        ix = cls._meili_index()
        ix.update_documents(docs)

    # ....................... #

    @classmethod
    async def ameili_update_documents(cls: Type[M], docs: List[Dict[str, Any]]):
        ix = await cls._ameili_index()
        await ix.update_documents(docs)

    # ....................... #

    @classmethod
    def meili_last_update(cls: Type[M]) -> Optional[int]:
        ix = cls._meili_index()
        dt = ix.updated_at

        if dt:
            return int(dt.timestamp())

        return None

    # ....................... #

    @classmethod
    async def ameili_last_update(cls: Type[M]) -> Optional[int]:
        ix = await cls._ameili_index()
        dt = ix.updated_at

        if dt:
            return int(dt.timestamp())

        return None
