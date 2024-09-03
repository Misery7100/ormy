from typing import ClassVar, Type, TypeVar, get_args

from infi.clickhouse_orm import engines, fields, models  # type: ignore[import-untyped]
from pydantic._internal._model_construction import ModelMetaclass

from ormwtf.base.abc import AbstractABC
from ormwtf.utils.logging import LogLevel, console_logger

from .config import ClickHouseConfig
from .database import AsyncDatabase

# ----------------------- #

Ch = TypeVar("Ch", bound="ClickHouseBase")
logger = console_logger(__name__, level=LogLevel.INFO)

# ....................... #


class ClickHouseBaseMetaclass(ModelMetaclass):
    def __new__(cls, name, bases, attrs):
        pass


# ....................... #


class ClickHouseBase(AbstractABC):

    configs = [ClickHouseConfig()]
    engine: ClassVar[engines.Engine] = None

    _registry = {ClickHouseConfig: {}}
    _model: ClassVar[models.Model] = None

    # ....................... #

    def __init_subclass__(cls: type[Ch], **kwargs):
        super().__init_subclass__(**kwargs)

        cls._clickhouse_register_subclass()
        cls._construct_model()
        cls._merge_registry()

        ClickHouseBase._registry = cls._merge_registry_helper(
            ClickHouseBase._registry,
            cls._registry,
        )

        cls._model.set_database(cls, cls._get_adatabase())

    # ....................... #

    @classmethod
    def _construct_model(cls: Type[Ch]):
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

    # ....................... #

    @classmethod
    def _clickhouse_register_subclass(cls: Type[Ch]):
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

        return AsyncDatabase(
            db_name=cfg.database,
            verify_ssl_cert=False,  # TODO: check
            username=username,
            password=password,
            db_url=cfg.db_url(),
        )
