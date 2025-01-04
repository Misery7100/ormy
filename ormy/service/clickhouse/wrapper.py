import inspect
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
)

from infi.clickhouse_orm import engines, fields  # type: ignore[import-untyped]

from ormy.base.abc import AbstractABC
from ormy.base.logging import LogManager

from .config import ClickHouseConfig
from .func import get_clickhouse_db
from .models import ClickHouseFieldInfo, ClickHouseModel, ClickHouseQuerySet

# ----------------------- #

Ch = TypeVar("Ch", bound="ClickHouseBase")
logger = LogManager.get_logger(__name__)

# ----------------------- #


class ClickHouseBase(AbstractABC):

    configs = [ClickHouseConfig()]
    engine: ClassVar[Optional[engines.Engine]] = None

    _registry = {ClickHouseConfig: {}}
    _model: ClassVar[Optional[ClickHouseModel]] = None  # type: ignore[assignment]

    # ....................... #

    def __init_subclass__(cls: type[Ch], **kwargs):
        super().__init_subclass__(**kwargs)

        cls.__clickhouse_register_subclass()
        cls.__construct_model()
        cls._merge_registry()

        ClickHouseBase._registry = cls._merge_registry_helper(
            ClickHouseBase._registry,
            cls._registry,
        )

        cls._model.set_database(cls, cls._get_adatabase())  # type: ignore

    # ....................... #

    @classmethod
    def __construct_model(cls: Type[Ch]):
        _dict_: Dict[str, Any] = {}
        orm_fields = {}
        engine = None

        parents = inspect.getmro(cls)

        for p in parents[::-1]:
            if issubclass(p, ClickHouseBase):
                for attr_name, attr_value in p.__dict__.items():
                    if isinstance(attr_value, ClickHouseFieldInfo) or isinstance(
                        attr_value, engines.Engine
                    ):
                        _dict_[attr_name] = attr_value

                    elif attr_name in ["model_fields", "__pydantic_fields__"]:
                        for k, v in attr_value.items():
                            if isinstance(v, ClickHouseFieldInfo):
                                _dict_[k] = v.clickhouse

        for attr_name, attr_value in _dict_.items():
            if isinstance(attr_value, ClickHouseFieldInfo):
                orm_fields[attr_name] = attr_value.clickhouse

            elif isinstance(attr_value, fields.Field):
                orm_fields[attr_name] = attr_value

            elif isinstance(attr_value, engines.Engine):
                engine = attr_value

        # Dynamically create the ORM model
        orm_attrs = {"engine": engine, **orm_fields}

        cls._model = type(f"{cls.__name__}_infi", (ClickHouseModel,), orm_attrs)  # type: ignore[assignment]
        setattr(
            cls._model,
            "table_name",
            lambda: cls.get_config(type_=ClickHouseConfig).table,
        )

    # ....................... #

    @classmethod
    def __clickhouse_register_subclass(cls: Type[Ch]):
        """Register subclass in the registry"""

        cfg = cls.get_config(type_=ClickHouseConfig)
        db = cfg.database
        table = cfg.table

        if cfg.include_to_registry and not cfg.is_default():
            logger.debug(f"Registering {cls.__name__} in {db}.{table}")
            logger.debug(f"Registry before: {cls._registry}")

            cls._registry[ClickHouseConfig] = cls._registry.get(ClickHouseConfig, {})
            cls._registry[ClickHouseConfig][db] = cls._registry[ClickHouseConfig].get(
                db, {}
            )
            cls._registry[ClickHouseConfig][db][table] = cls

            logger.debug(f"Registry after: {cls._registry}")

    # ....................... #

    @classmethod
    def _get_adatabase(cls: Type[Ch]):
        """
        Get ClickHouse database connection
        """

        cfg = cls.get_config(type_=ClickHouseConfig)

        username = (
            cfg.credentials.username.get_secret_value()
            if cfg.credentials.username
            else None
        )
        password = (
            cfg.credentials.password.get_secret_value()
            if cfg.credentials.password
            else None
        )

        return get_clickhouse_db(
            db_name=cfg.database,
            username=username,
            password=password,
            db_url=cfg.url(),
        )

    # ....................... #

    @classmethod
    def objects(cls: Type[Ch]) -> ClickHouseQuerySet:
        return cls._model.objects_in(cls._get_adatabase())  # type: ignore

    # ....................... #

    @classmethod
    def _get_materialized_fields(cls: Type[Ch]):
        fields = []

        for x, v in cls.model_fields.items():
            if v.clickhouse.materialized:  # type: ignore[attr-defined]
                fields.append(x)

        return fields

    # ....................... #

    @classmethod
    def insert(
        cls: Type[Ch],
        records: Ch | List[Ch],
        batch_size: int = 1000,
    ) -> None:
        if not isinstance(records, list):
            records = [records]

        model_records = [
            cls._model(
                **record.model_dump(
                    exclude=cls._get_materialized_fields(),  # type: ignore
                )
            )  # type: ignore
            for record in records
        ]
        return cls._get_adatabase().insert(model_records, batch_size=batch_size)

    # ....................... #

    @classmethod
    async def ainsert(
        cls: Type[Ch],
        records: Ch | List[Ch],
        batch_size: int = 1000,
    ) -> None:
        if not isinstance(records, list):
            records = [records]

        model_records = [
            cls._model(
                **record.model_dump(
                    exclude=cls._get_materialized_fields(),  # type: ignore
                )
            )  # type: ignore
            for record in records
        ]
        return await cls._get_adatabase().ainsert(model_records, batch_size=batch_size)

    # ....................... #
