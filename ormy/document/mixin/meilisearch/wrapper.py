import asyncio
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Callable, ClassVar, Optional, Self, Type, TypeVar

from ormy._abc.registry import Registry
from ormy.base.typing import AsyncCallable
from ormy.document._abc import DocumentMixinABC
from ormy.exceptions import ModuleNotFound

try:
    from meilisearch_python_sdk import AsyncClient, Client
    from meilisearch_python_sdk.errors import MeilisearchApiError
    from meilisearch_python_sdk.models.settings import MeilisearchSettings
    from meilisearch_python_sdk.types import JsonDict
except ImportError as e:
    raise ModuleNotFound(
        extra="meilisearch", packages=["meilisearch-python-sdk"]
    ) from e

from .config import MeilisearchConfig
from .schema import (
    AnyFilter,
    ArrayFilter,
    BooleanFilter,
    DatetimeFilter,
    MeilisearchReference,
    NumberFilter,
    SearchRequest,
    SearchRequestDict,
    SearchResponse,
    SortField,
)

# ----------------------- #

T = TypeVar("T")

# ----------------------- #


class MeilisearchMixin(DocumentMixinABC):
    """Meilisearch mixin"""

    mixin_configs: ClassVar[list[Any]] = [MeilisearchConfig()]

    __meili_static: ClassVar[Optional[Client]] = None
    __ameili_static: ClassVar[Optional[AsyncClient]] = None

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)

        cls.defer_mixin_registration(
            config=MeilisearchConfig,
            discriminator="index",
        )

    # ....................... #

    @classmethod  # TODO: remove ? or simplify somehow
    def meili_model_reference(cls):
        """
        Generate a Meilisearch reference for the model schema with filters and sort fields

        Returns:
            schema (MeilisearchReference): The Meilisearch reference for the model schema
        """

        full_schema = cls.model_flat_schema()
        cfg = cls.get_mixin_config(type_=MeilisearchConfig)

        sort = []
        filters = []

        if filterable := cfg.settings.filterable_attributes:
            for f in filterable:
                if field := next((x for x in full_schema if x["key"] == f), None):
                    filter_model: Optional[Type[AnyFilter]] = None

                    match field["type"]:
                        case "boolean":
                            filter_model = BooleanFilter

                        case "number":
                            filter_model = NumberFilter

                        case "integer":
                            filter_model = NumberFilter

                        case "datetime":
                            filter_model = DatetimeFilter

                        case "array":
                            filter_model = ArrayFilter

                        case _:
                            field["type"] = "array"
                            filter_model = ArrayFilter

                    if filter_model:
                        filters.append(filter_model.model_validate(field))

        if sortable := cfg.settings.sortable_attributes:
            default_sort = cfg.settings.default_sort

            for s in sortable:
                if field := next((x for x in full_schema if x["key"] == s), None):
                    sort_key = SortField(**field, default=s == default_sort)
                    sort.append(sort_key)

        return MeilisearchReference(
            filters=filters,
            sort=sort,
        )

    # ....................... #

    @classmethod
    def __is_static_meili(cls):
        """
        Check if static Meilisearch client is used

        Returns:
            use_static (bool): Whether to use static Meilisearch client
        """

        cfg = cls.get_mixin_config(type_=MeilisearchConfig)
        use_static = not cfg.context_client

        return use_static

    # ....................... #

    @classmethod
    def __get_exclude_mask(cls):
        """Get exclude mask"""

        cfg = cls.get_mixin_config(type_=MeilisearchConfig)
        return cfg.settings.exclude_mask

    # ....................... #

    @classmethod
    def __meili_abstract_client(cls):
        """Abstract client"""

        cfg = cls.get_mixin_config(type_=MeilisearchConfig)
        url = cfg.url()
        key = cfg.credentials.master_key

        if key:
            api_key = key.get_secret_value()

        else:
            api_key = None

        return Client(
            url=url,
            api_key=api_key,
            custom_headers={"Content-Type": "application/json"},
        )

    # ....................... #

    @classmethod
    def __ameili_abstract_client(cls):
        """Abstract async client"""

        cfg = cls.get_mixin_config(type_=MeilisearchConfig)
        url = cfg.url()
        key = cfg.credentials.master_key

        if key:
            api_key = key.get_secret_value()

        else:
            api_key = None

        return AsyncClient(
            url=url,
            api_key=api_key,
            custom_headers={"Content-Type": "application/json"},
        )

    # ....................... #

    @classmethod
    def _meili_static_client(cls):
        """
        Get static Meilisearch client

        Returns:
            client (meilisearch_python_sdk.Client): Static Meilisearch client
        """

        if cls.__meili_static is None:
            cls.__meili_static = cls.__meili_abstract_client()

        return cls.__meili_static

    # ....................... #

    @classmethod
    def _ameili_static_client(cls):
        """
        Get static async Meilisearch client

        Returns:
            client (meilisearch_python_sdk.AsyncClient): Static async Meilisearch client
        """

        if cls.__ameili_static is None:
            cls.__ameili_static = cls.__ameili_abstract_client()

        return cls.__ameili_static

    # ....................... #

    @classmethod
    def __meili_execute_task(cls, task: Callable[[Any], T]) -> T:
        """Execute task"""

        if cls.__is_static_meili():
            c = cls._meili_static_client()
            return task(c)

        else:
            with cls._meili_client() as c:
                return task(c)

    # ....................... #

    @classmethod
    async def __ameili_execute_task(cls, task: AsyncCallable[[Any], T]) -> T:
        """Execute async task"""

        if cls.__is_static_meili():
            c = cls._ameili_static_client()
            return await task(c)

        else:
            async with cls._ameili_client() as c:
                return await task(c)

    # ....................... #

    @classmethod
    def meili_safe_create_or_update(cls):
        """
        Safely create or update the Meilisearch index.
        If the index does not exist, it will be created.
        If the index exists and settings were updated, index will be updated.
        """

        cfg = cls.get_mixin_config(type_=MeilisearchConfig)

        def _task(c: Client):
            try:
                ix = c.get_index(cfg.index)
                cls._logger().debug(f"Index `{cfg.index}` already exists")
                settings = MeilisearchSettings.model_validate(cfg.settings.model_dump())

                if ix.get_settings() != settings:
                    ix.update_settings(settings)
                    cls._logger().debug(f"Update of index `{cfg.index}` is started")

            except MeilisearchApiError:
                settings = MeilisearchSettings.model_validate(cfg.settings.model_dump())
                c.create_index(
                    cfg.index,
                    primary_key=cfg.primary_key,
                    settings=settings,
                )
                cls._logger().debug(f"Index `{cfg.index}` is created")

        if not cfg.is_default():
            cls.__meili_execute_task(_task)

    # ....................... #

    @classmethod
    async def ameili_safe_create_or_update(cls):
        """
        Safely create or update the Meilisearch index.
        If the index does not exist, it will be created.
        If the index exists and settings were updated, index will be updated.
        """

        cfg = cls.get_mixin_config(type_=MeilisearchConfig)

        async def _task(c: AsyncClient):
            try:
                ix = await c.get_index(cfg.index)
                cls._logger().debug(f"Index `{cfg.index}` already exists")
                settings = MeilisearchSettings.model_validate(cfg.settings.model_dump())

                if (await ix.get_settings()) != settings:
                    await ix.update_settings(settings)
                    cls._logger().debug(f"Update of index `{cfg.index}` is started")

            except MeilisearchApiError:
                settings = MeilisearchSettings.model_validate(cfg.settings.model_dump())
                await c.create_index(
                    cfg.index,
                    primary_key=cfg.primary_key,
                    settings=settings,
                )
                cls._logger().debug(f"Index `{cfg.index}` is created")

        if not cfg.is_default():
            await cls.__ameili_execute_task(_task)

    # ....................... #

    @classmethod  # TODO: move above
    @contextmanager
    def _meili_client(cls):
        """Get syncronous Meilisearch client"""

        try:
            yield cls.__meili_abstract_client()

        finally:
            pass

    # ....................... #

    @classmethod  # TODO: move above
    @asynccontextmanager
    async def _ameili_client(cls):
        """Get asyncronous Meilisearch client"""

        try:
            yield cls.__ameili_abstract_client()

        finally:
            pass

    # ....................... #

    @classmethod
    def _meili_health(cls) -> bool:
        """Check Meilisearch health"""

        def _task(c: Client):
            try:
                h = c.health()
                status = h.status == "available"

            except Exception:
                status = False

            return status

        return cls.__meili_execute_task(_task)

    # ....................... #

    @classmethod
    async def _ameili_health(cls) -> bool:
        """Check Meilisearch health"""

        async def _task(c: AsyncClient):
            try:
                h = await c.health()
                status = h.status == "available"

            except Exception:
                status = False

            return status

        return await cls.__ameili_execute_task(_task)

    # ....................... #

    @classmethod
    def _meili_index(cls):
        """Get associated Meilisearch index"""

        cfg = cls.get_mixin_config(type_=MeilisearchConfig)

        def _task(c: Client):
            return c.get_index(cfg.index)

        return cls.__meili_execute_task(_task)

    # ....................... #

    @classmethod
    async def _ameili_index(cls):
        """Get associated Meilisearch index in asyncronous mode"""

        cfg = cls.get_mixin_config(type_=MeilisearchConfig)

        async def _task(c: AsyncClient):
            return await c.get_index(cfg.index)

        return await cls.__ameili_execute_task(_task)

    # ....................... #

    @classmethod
    def _meili_prepare_request(
        cls,
        request: SearchRequest | SearchRequestDict,
        page: int = 1,
        size: int = 20,
    ):
        """
        Prepare search request

        Args:
            request (SearchRequest | SearchRequestDict): The search request
            page (int, optional): The page number
            size (int, optional): The number of hits per page

        Returns:
            request (dict): The prepared search request
        """

        cfg = cls.get_mixin_config(type_=MeilisearchConfig)
        sortable = cfg.settings.sortable_attributes
        filterable = cfg.settings.filterable_attributes

        if sortable is None:
            sortable = []

        if isinstance(request, dict):
            request = SearchRequest.model_validate(request)

        if request.sort and request.sort in sortable:
            sort = [f"{request.sort}:{request.order.value}"]

        else:
            sort = None

        if request.filters and filterable:
            filters = [
                f.build()
                for f in request.filters
                if f.key in filterable or filterable == ["*"]
            ]
            filters = list(filter(None, filters))

        else:
            filters = []

        return {
            "query": request.query,
            "hits_per_page": size,
            "page": page,
            "sort": sort,
            "filter": filters,
        }

    # ....................... #

    @staticmethod
    def _meili_prepare_response(res: Any):
        """
        Prepare search response

        Args:
            res (meilisearch_python_sdk.models.search.SearchResults): The search results

        Returns:
            response (SearchResponse): The prepared search response
        """

        return SearchResponse.from_search_results(res)

    # ....................... #

    @classmethod
    def meili_search(
        cls,
        request: SearchRequest | SearchRequestDict,
        page: int = 1,
        size: int = 20,
        include: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
    ):
        """
        Search documents in Meilisearch

        Args:
            request (SearchRequest | SearchRequestDict): The search request
            page (int, optional): The page number
            size (int, optional): The number of hits per page
            include (list[str], optional): The fields to include in the search
            exclude (list[str], optional): The fields to exclude from the search

        Returns:
            response (SearchResponse): The search response
        """

        fields = list(cls.model_fields.keys()) + list(cls.model_computed_fields.keys())

        if exclude is not None and include is None:
            include = [x for x in fields if x not in exclude]

        elif include is not None:
            include = [x for x in include if x in fields]

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
        cls,
        request: SearchRequest | SearchRequestDict,
        page: int = 1,
        size: int = 20,
        include: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
    ):
        """
        Search documents in Meilisearch in asyncronous mode

        Args:
            request (SearchRequest | SearchRequestDict): The search request
            page (int, optional): The page number
            size (int, optional): The number of hits per page
            include (list[str], optional): The fields to include in the search
            exclude (list[str], optional): The fields to exclude from the search

        Returns:
            response (SearchResponse): The search response
        """

        fields = list(cls.model_fields.keys()) + list(cls.model_computed_fields.keys())

        if exclude is not None and include is None:
            include = [x for x in fields if x not in exclude]

        elif include is not None:
            include = [x for x in include if x in fields]

        ix = await cls._ameili_index()
        req = cls._meili_prepare_request(request, page, size)
        res = await ix.search(
            attributes_to_retrieve=include,
            **req,
        )

        return cls._meili_prepare_response(res)

    # ....................... #

    @classmethod
    def meili_delete_documents(cls, ids: str | list[str]):
        """
        Delete documents from Meilisearch

        Args:
            ids (str | list[str]): The document IDs
        """

        ix = cls._meili_index()

        if isinstance(ids, str):
            ids = [ids]

        ix.delete_documents(ids)

    # ....................... #

    @classmethod
    async def ameili_delete_documents(cls, ids: str | list[str]):
        """
        Delete documents from Meilisearch in asyncronous mode

        Args:
            ids (str | list[str]): The document IDs
        """

        ix = await cls._ameili_index()

        if isinstance(ids, str):
            ids = [ids]

        await ix.delete_documents(ids)

    # ....................... #

    @classmethod
    def _meili_all_documents(cls):
        """
        Get all documents from Meilisearch

        Returns:
            documents (list[JsonDict]): The list of documents
        """

        ix = cls._meili_index()
        res: list[JsonDict] = []
        offset = 0

        while docs := ix.get_documents(offset=offset, limit=1000).results:
            res.extend(docs)
            offset += 1000

        return res

    # ....................... #

    @classmethod
    async def _ameili_all_documents(cls):
        """
        Get all documents from Meilisearch in asyncronous mode

        Returns:
            documents (list[JsonDict]): The list of documents
        """

        ix = await cls._ameili_index()
        res: list[JsonDict] = []
        offset = 0

        while docs := (await ix.get_documents(offset=offset, limit=1000)).results:
            res.extend(docs)
            offset += 1000

        return res

    # ....................... #

    @classmethod
    def meili_update_documents(cls, docs: Self | list[Self]):
        """
        Update documents in Meilisearch

        Args:
            docs (M | list[M]): The documents to update
        """

        ix = cls._meili_index()
        exclude_mask = cls.__get_exclude_mask()

        if not isinstance(docs, list):
            docs = [docs]

        masked = []

        if exclude_mask:
            for d in docs:
                for k, v in exclude_mask.items():
                    if hasattr(d, k):
                        doc_value = getattr(d, k)

                        if not isinstance(v, list):
                            v = [v]

                        if not isinstance(doc_value, list):
                            doc_value = [doc_value]

                        # Handle unhashable exceptions
                        try:
                            if set(doc_value).intersection(v):
                                masked.append(d)

                        except Exception:
                            pass

        doc_dicts = [d.model_dump() for d in docs if d not in masked]
        ix.update_documents(doc_dicts)

    # ....................... #

    @classmethod
    async def ameili_update_documents(cls, docs: Self | list[Self]):
        """
        Update documents in Meilisearch in asyncronous mode

        Args:
            docs (M | list[M]): The documents to update
        """

        ix = await cls._ameili_index()
        exclude_mask = cls.__get_exclude_mask()

        if not isinstance(docs, list):
            docs = [docs]

        masked = []

        if exclude_mask:
            for d in docs:
                for k, v in exclude_mask.items():
                    if hasattr(d, k):
                        doc_value = getattr(d, k)

                        if not isinstance(v, list):
                            v = [v]

                        if not isinstance(doc_value, list):
                            doc_value = [doc_value]

                        # Handle unhashable exceptions
                        try:
                            if set(doc_value).intersection(v):
                                masked.append(d)

                        except Exception:
                            pass

        doc_dicts = [d.model_dump() for d in docs if d not in masked]
        await ix.update_documents(doc_dicts)

    # ....................... #

    @classmethod
    def meili_last_update(cls):
        """
        Get the last update timestamp of the Meilisearch index

        Returns:
            timestamp (int | None): The last update timestamp
        """

        ix = cls._meili_index()
        dt = ix.updated_at

        if dt:
            return int(dt.timestamp())

        return None

    # ....................... #

    @classmethod
    async def ameili_last_update(cls):
        """
        Get the last update timestamp of the Meilisearch index in asyncronous mode

        Returns:
            timestamp (int | None): The last update timestamp
        """

        ix = await cls._ameili_index()
        dt = ix.updated_at

        if dt:
            return int(dt.timestamp())

        return None

    # ....................... #

    @staticmethod
    def meili_safe_init(*entries: "MeilisearchMixin"):
        """
        Safe create or update indexes

        Args:
            entries (tuple[MeilisearchMixin]): The entries to initialize
        """

        if not entries:
            entries: list[MeilisearchMixin] = Registry.get_by_config(MeilisearchConfig)  # type: ignore[no-redef]

        for x in entries:
            x.meili_safe_create_or_update()

    # ....................... #

    @staticmethod
    async def ameili_safe_init(*entries: "MeilisearchMixin"):
        """
        Safe create or update indexes in asyncronous mode

        Args:
            entries (tuple[MeilisearchMixin]): The entries to initialize
        """

        if not entries:
            entries: list[MeilisearchMixin] = Registry.get_by_config(MeilisearchConfig)  # type: ignore[no-redef]

        tasks = [x.ameili_safe_create_or_update() for x in entries]

        if tasks:
            await asyncio.gather(*tasks)
