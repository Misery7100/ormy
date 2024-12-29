from contextlib import asynccontextmanager, contextmanager
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
)

from meilisearch_python_sdk import AsyncClient, AsyncIndex, Client, Index
from meilisearch_python_sdk.errors import MeilisearchApiError
from meilisearch_python_sdk.models.search import SearchResults
from meilisearch_python_sdk.models.settings import MeilisearchSettings
from meilisearch_python_sdk.types import JsonDict

from ormy.base.abc import ExtensionABC
from ormy.base.typing import AsyncCallable
from ormy.utils.logging import LogLevel, console_logger

from .config import MeilisearchConfig
from .schema import (
    ArrayFilter,
    BooleanFilter,
    DatetimeFilter,
    MeilisearchReference,
    NumberFilter,
    SearchRequest,
    SearchResponse,
    SomeFilter,
    SortField,
)

# ----------------------- #

M = TypeVar("M", bound="MeilisearchExtensionV2")
logger = console_logger(__name__, level=LogLevel.INFO)

# ----------------------- #


class MeilisearchExtensionV2(ExtensionABC):
    """Meilisearch extension"""

    extension_configs: ClassVar[List[Any]] = [MeilisearchConfig()]
    _registry = {MeilisearchConfig: {}}

    _meili_static: ClassVar[Optional[Client]] = None
    _ameili_static: ClassVar[Optional[AsyncClient]] = None

    # ....................... #

    def __init_subclass__(cls: Type[M], **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)

        cls._meili_register_subclass()
        cls._merge_registry()
        cls._meili_safe_create_or_update()

        MeilisearchExtensionV2._registry = cls._merge_registry_helper(
            MeilisearchExtensionV2._registry,
            cls._registry,
        )

    # ....................... #

    @classmethod
    def _meili_register_subclass(cls: Type[M]):
        """Register subclass in the registry"""

        return cls._register_subclass_helper(
            config=MeilisearchConfig,
            discriminator="index",
        )

    # ....................... #

    @classmethod  # TODO: remove ? or simplify somehow
    def meili_model_reference(
        cls: Type[M],
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        extra: Optional[List[str]] = None,
        extra_definitions: List[Dict[str, str]] = [],
    ) -> MeilisearchReference:
        """
        Generate a Meilisearch reference for the model schema with filters and sort fields

        Args:
            include (List[str], optional): The fields to include in the schema.
            exclude (List[str], optional): The fields to exclude from the schema.
            extra (List[str], optional): Extra fields to include in the schema.
            extra_definitions (List[Dict[str, str]], optional): Extra definitions for the schema.

        Returns:
            schema (MeilisearchReference): The Meilisearch reference for the model schema
        """

        table_schema = cls.model_flat_schema(
            include=include,
            exclude=exclude,
            extra=extra,
            extra_definitions=extra_definitions,
        )

        full_schema = cls.model_flat_schema()
        cfg = cls.get_extension_config(type_=MeilisearchConfig)

        sort = []
        filters = []

        if filterable := cfg.settings.filterable_attributes:
            for f in filterable:
                if field := next((x for x in full_schema if x["key"] == f), None):
                    filter_model: Optional[Type[SomeFilter]] = None

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
                            pass

                    if filter_model:
                        filters.append(filter_model.model_validate(field))

        if sortable := cfg.settings.sortable_attributes:
            default_sort = cfg.settings.default_sort

            for s in sortable:
                if field := next((x for x in full_schema if x["key"] == s), None):
                    sort_key = SortField(**field, default=s == default_sort)
                    sort.append(sort_key)

        return MeilisearchReference(
            table_schema=table_schema,
            filters=filters,
            sort=sort,
        )

    # ....................... #

    @classmethod
    def __is_static_meili(cls: Type[M]):
        """Check if static Meilisearch client is used"""

        cfg = cls.get_extension_config(type_=MeilisearchConfig)
        use_static = not cfg.context_client

        return use_static

    # ....................... #

    @classmethod
    def _meili_static_client(cls):
        """Get static Meilisearch client"""

        health = False

        if cls._meili_static is not None:
            try:
                check = cls._meili_static.health()
                health = check.status == "available"

            except Exception:
                pass

        if not health or cls._meili_static is None:
            cfg = cls.get_extension_config(type_=MeilisearchConfig)
            url = cfg.url()
            key = cfg.credentials.master_key

            if key:
                api_key = key.get_secret_value()

            else:
                api_key = None

            cls._meili_static = Client(
                url=url,
                api_key=api_key,
                custom_headers={"Content-Type": "application/json"},
            )

        return cls._meili_static

    # ....................... #

    @classmethod
    async def _ameili_static_client(cls):
        """Get static async Meilisearch client"""

        health = False

        if cls._ameili_static is not None:
            try:
                check = await cls._ameili_static.health()
                health = check.status == "available"

            except Exception:
                pass

        if not health or cls._ameili_static is None:
            cfg = cls.get_extension_config(type_=MeilisearchConfig)
            url = cfg.url()
            key = cfg.credentials.master_key

            if key:
                api_key = key.get_secret_value()

            else:
                api_key = None

            cls._ameili_static = AsyncClient(
                url=url,
                api_key=api_key,
                custom_headers={"Content-Type": "application/json"},
            )

        return cls._ameili_static

    # ....................... #

    @classmethod
    def __meili_execute_task(
        cls: Type[M],
        task: Callable[[Client], Any],
    ):
        """Execute task"""

        if cls.__is_static_meili():
            c = cls._meili_static_client()
            return task(c)

        else:
            with cls._meili_client() as c:
                return task(c)

    # ....................... #

    @classmethod
    async def __ameili_execute_task(
        cls: Type[M],
        task: AsyncCallable[AsyncClient, Any],
    ):
        """Execute async task"""

        if cls.__is_static_meili():
            c = await cls._ameili_static_client()
            return await task(c)

        else:
            async with cls._ameili_client() as c:
                return await task(c)

    # ....................... #

    @classmethod
    def _meili_safe_create_or_update(cls: Type[M]):
        """
        Safely create or update the Meilisearch index.
        If the index does not exist, it will be created.
        If the index exists and settings were updated, index will be updated.
        """

        cfg = cls.get_extension_config(type_=MeilisearchConfig)

        def _task(c: Client):
            try:
                ix = c.get_index(cfg.index)
                logger.debug(f"Index `{cfg.index}` already exists")
                settings = MeilisearchSettings.model_validate(cfg.settings.model_dump())

                if ix.get_settings() != settings:
                    cls._meili_update_index(settings)
                    logger.debug(f"Update of index `{cfg.index}` is started")

            except MeilisearchApiError:
                settings = MeilisearchSettings.model_validate(cfg.settings.model_dump())
                c.create_index(
                    cfg.index,
                    primary_key=cfg.primary_key,
                    settings=settings,
                )
                logger.debug(f"Index `{cfg.index}` is created")

        if not cfg.is_default():
            cls.__meili_execute_task(_task)

    # ....................... #

    @classmethod  # TODO: move above
    @contextmanager
    def _meili_client(cls):
        """Get syncronous Meilisearch client"""

        cfg = cls.get_extension_config(type_=MeilisearchConfig)
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

    @classmethod  # TODO: move above
    @asynccontextmanager
    async def _ameili_client(cls):
        """Get asyncronous Meilisearch client"""

        cfg = cls.get_extension_config(type_=MeilisearchConfig)
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
    def _meili_index(cls: Type[M]) -> Index:
        """Get associated Meilisearch index"""

        cfg = cls.get_extension_config(type_=MeilisearchConfig)

        def _task(c: Client):
            return c.get_index(cfg.index)

        return cls.__meili_execute_task(_task)

    # ....................... #

    @classmethod
    async def _ameili_index(cls: Type[M]) -> AsyncIndex:
        """Get associated Meilisearch index in asyncronous mode"""

        cfg = cls.get_extension_config(type_=MeilisearchConfig)

        async def _task(c: AsyncClient):
            return await c.get_index(cfg.index)

        return await cls.__ameili_execute_task(_task)

    # ....................... #

    @classmethod
    def _meili_update_index(cls: Type[M], settings: MeilisearchSettings):
        """
        Update Meilisearch index settings

        Args:
            settings (MeilisearchSettings): The settings to update
        """

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
        """
        Prepare search request

        Args:
            request (SearchRequest): The search request
            page (int, optional): The page number
            size (int, optional): The number of hits per page

        Returns:
            request (dict): The prepared search request
        """

        cfg = cls.get_extension_config(type_=MeilisearchConfig)
        sortable = cfg.settings.sortable_attributes
        filterable = cfg.settings.filterable_attributes

        if sortable is None:
            sortable = []

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
    def _meili_prepare_response(res: SearchResults) -> SearchResponse:
        """
        Prepare search response

        Args:
            res (SearchResults): The search results

        Returns:
            response (SearchResponse): The prepared search response
        """

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
        Search documents in Meilisearch

        Args:
            request (SearchRequest): The search request
            page (int, optional): The page number
            size (int, optional): The number of hits per page
            include (List[str], optional): The fields to include in the search
            exclude (List[str], optional): The fields to exclude from the search

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
        cls: Type[M],
        request: SearchRequest,
        page: int = 1,
        size: int = 20,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> SearchResponse:
        """
        Search documents in Meilisearch in asyncronous mode

        Args:
            request (SearchRequest): The search request
            page (int, optional): The page number
            size (int, optional): The number of hits per page
            include (List[str], optional): The fields to include in the search
            exclude (List[str], optional): The fields to exclude from the search

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
    def meili_delete_documents(cls: Type[M], ids: str | List[str]):
        """
        Delete documents from Meilisearch

        Args:
            ids (str | List[str]): The document IDs
        """

        ix = cls._meili_index()

        if isinstance(ids, str):
            ids = [ids]

        ix.delete_documents(ids)

    # ....................... #

    @classmethod
    async def ameili_delete_documents(cls: Type[M], ids: str | List[str]):
        """
        Delete documents from Meilisearch in asyncronous mode

        Args:
            ids (str | List[str]): The document IDs
        """

        ix = await cls._ameili_index()

        if isinstance(ids, str):
            ids = [ids]

        await ix.delete_documents(ids)

    # ....................... #

    @classmethod
    def _meili_all_documents(cls: Type[M]):
        """
        Get all documents from Meilisearch

        Returns:
            documents (List[JsonDict]): The list of documents
        """

        ix = cls._meili_index()
        res: List[JsonDict] = []
        offset = 0

        while docs := ix.get_documents(offset=offset, limit=1000).results:
            res.extend(docs)
            offset += 1000

        return res

    # ....................... #

    @classmethod
    async def _ameili_all_documents(cls: Type[M]):
        """
        Get all documents from Meilisearch in asyncronous mode

        Returns:
            documents (List[JsonDict]): The list of documents
        """

        ix = await cls._ameili_index()
        res: List[JsonDict] = []
        offset = 0

        while docs := (await ix.get_documents(offset=offset, limit=1000)).results:
            res.extend(docs)
            offset += 1000

        return res

    # ....................... #

    @classmethod
    def meili_update_documents(cls: Type[M], docs: M | List[M]):
        """
        Update documents in Meilisearch

        Args:
            docs (M | List[M]): The documents to update
        """

        ix = cls._meili_index()

        if not isinstance(docs, list):
            docs = [docs]

        doc_dicts = [d.model_dump() for d in docs]
        ix.update_documents(doc_dicts)

    # ....................... #

    @classmethod
    async def ameili_update_documents(cls: Type[M], docs: M | List[M]):
        """
        Update documents in Meilisearch in asyncronous mode

        Args:
            docs (M | List[M]): The documents to update
        """

        ix = await cls._ameili_index()

        if not isinstance(docs, list):
            docs = [docs]

        doc_dicts = [d.model_dump() for d in docs]
        await ix.update_documents(doc_dicts)

    # ....................... #

    @classmethod
    def meili_last_update(cls: Type[M]) -> Optional[int]:
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
    async def ameili_last_update(cls: Type[M]) -> Optional[int]:
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
