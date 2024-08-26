import inspect  # noqa: F401
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Dict, List, Optional, Type, TypeVar

from meilisearch_python_sdk import AsyncClient, AsyncIndex, Client, Index
from meilisearch_python_sdk.errors import MeilisearchApiError
from meilisearch_python_sdk.models.search import SearchResults
from meilisearch_python_sdk.models.settings import MeilisearchSettings
from meilisearch_python_sdk.types import JsonDict

from ormwtf.base.func import _merge_config_with_parent
from ormwtf.base.pydantic import BaseModel
from ormwtf.utils.logging import LogLevel, console_logger

from .config import MeilisearchConfig
from .schema import SearchRequest, SearchResponse

# ----------------------- #

M = TypeVar("M", bound="MeilisearchExtension")
logger = console_logger(__name__, level=LogLevel.DEBUG)

# ....................... #


class MeilisearchExtension(BaseModel):

    meili_config: ClassVar[MeilisearchConfig] = MeilisearchConfig()
    _meili_registry: ClassVar[Dict[str, Any]] = {}

    # ....................... #

    def __init_subclass__(cls: Type[M], **kwargs):
        super().__init_subclass__(**kwargs)
        _merge_config_with_parent(
            cls,
            "meili_config",
            MeilisearchConfig,
            inspect_ignored=False,
        )

        # # TODO: move to base utils ?
        # parents = inspect.getmro(cls)[1:]
        # nearest = None
        # config_key = "meili_config"
        # config_type = MeilisearchConfig

        # for p in parents:
        #     cfg = getattr(p, config_key, None)

        #     if type(cfg) is config_type:
        #         nearest = p
        #         break

        # if (nearest is not None) and (
        #     (nearest_config := getattr(nearest, config_key, None)) is not None
        # ):
        #     cls_config = getattr(cls, config_key)
        #     values = {**nearest_config.model_dump(), **cls_config.model_dump()}
        #     setattr(cls, config_key, type(cls_config)(**values))

        cls._meili_safe_create_or_update()
        cls._meili_register_subclass()

    # ....................... #

    @classmethod
    def _meili_register_subclass(cls: Type[M]):
        """Register subclass in the registry"""
        if cls.meili_config.include_to_registry:
            ix = cls.meili_config.index
            cls._meili_registry[ix] = cls

    # ....................... #

    @classmethod
    def _meili_safe_create_or_update(cls: Type[M]):
        if (
            cls.meili_config.index != "default"
        ):  # TODO: use exact default value from class
            with cls._meili_client() as c:
                try:
                    ix = c.get_index(cls.meili_config.index)
                    logger.info(f"Index {cls.meili_config.index} exists")

                    if ix.get_settings() != cls.meili_config.settings:
                        cls._meili_update_index(cls.meili_config.settings)
                        logger.info(f"Index {cls.meili_config.index} updated")

                except MeilisearchApiError:
                    c.create_index(
                        cls.meili_config.index,
                        primary_key=cls.meili_config.primary_key,
                        settings=cls.meili_config.settings,
                    )
                    logger.info(f"Index {cls.meili_config.index} created")

    # ....................... #

    @classmethod
    @contextmanager
    def _meili_client(cls: Type[M]):
        """Get syncronous Meilisearch client"""

        url = cls.meili_config.url()
        key = cls.meili_config.credentials.master_key

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

        url = cls.meili_config.url()
        key = cls.meili_config.credentials.master_key

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

        with cls._meili_client() as c:
            return c.get_index(cls.meili_config.index)

    # ....................... #

    @classmethod
    async def _ameili_index(cls: Type[M]) -> AsyncIndex:
        """Get associated Meilisearch index in asyncronous mode"""

        async with cls._ameili_client() as c:
            return await c.get_index(cls.meili_config.index)

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

        sortable = cls.meili_config.settings.sortable_attributes

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
    def _meili_prepare_response(res: SearchResults):
        return SearchResponse.model_validate(res, from_attributes=True)

    # ....................... #

    @classmethod
    def meili_search(
        cls: Type[M],
        request: SearchRequest,
        page: int = 1,
        size: int = 20,
        include: List[str] = ["*"],
    ):
        ix = cls._meili_index()
        req = cls._meili_prepare_request(request, page, size)
        res = ix.search(attributes_to_retrieve=include, **req)

        return cls._meili_prepare_response(res)

    # ....................... #

    @classmethod
    async def ameili_search(
        cls: Type[M],
        request: SearchRequest,
        page: int = 1,
        size: int = 20,
        include: List[str] = ["*"],
    ):
        ix = await cls._ameili_index()
        req = cls._meili_prepare_request(request, page, size)
        res = await ix.search(attributes_to_retrieve=include, **req)

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
