from typing import Any, ClassVar, Dict, List, Type, TypeVar, get_args

from infi.clickhouse_orm import engines, fields, models  # type: ignore[import-untyped]

from ormwtf.base.abc import AbstractABC
from ormwtf.utils.logging import LogLevel, console_logger

from .config import ClickHouseConfig
from .func import get_clickhouse_db

# ----------------------- #

Ch = TypeVar("Ch", bound="ClickHouseBase")
logger = console_logger(__name__, level=LogLevel.INFO)

# ....................... #


class ClickHouseBase(AbstractABC):

    configs = [ClickHouseConfig()]
    engine: ClassVar[engines.Engine] = None

    _registry = {ClickHouseConfig: {}}
    _model: ClassVar[models.Model] = None

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

        cls._model.set_database(cls, cls._get_adatabase())

    # ....................... #

    @classmethod
    def __construct_model(cls: Type[Ch]):
        orm_fields = {}
        engine = None

        for attr_name, attr_value in cls.__dict__.items():
            if annot := cls.__annotations__.get(attr_name, None):
                aargs = get_args(annot)

                if len(aargs) > 1 and isinstance(aargs[1], fields.Field):
                    orm_fields[attr_name] = aargs[1]

            elif isinstance(attr_value, fields.Field):
                orm_fields[attr_name] = attr_value

            elif isinstance(attr_value, engines.Engine):
                engine = attr_value

        # Dynamically create the ORM model
        orm_attrs = {"engine": engine, **orm_fields}

        cls._model = type(f"{cls.__name__}_infi", (models.Model,), orm_attrs)
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
            db_url=cfg.db_url(),
        )

    # ....................... #

    @classmethod
    def objects(cls: Type[Ch]) -> models.QuerySet:
        return cls._model.objects_in(cls._get_adatabase())

    # ....................... #

    @classmethod
    def evaluate(
        cls: Type[Ch],
        qs: models.QuerySet,
        raw: bool = False,
    ) -> List[Ch | Dict[str, Any]]:
        if raw:
            return [r.to_dict() for r in qs]  # type: ignore

        return [cls.model_validate_universal(r.to_dict()) for r in qs]  # type: ignore

    # ....................... #

    @classmethod
    def insert(
        cls: Type[Ch],
        records: Ch | List[Ch],
        batch_size: int = 1000,
    ) -> None:
        if not isinstance(records, list):
            records = [records]

        model_records = [cls._model(**record.model_dump()) for record in records]
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

        model_records = [cls._model(**record.model_dump()) for record in records]
        return await cls._get_adatabase().ainsert(model_records, batch_size=batch_size)

    # ....................... #
