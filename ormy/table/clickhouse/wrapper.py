import inspect
from typing import Any, ClassVar, Optional, Self

from ormy.exceptions import InternalError, ModuleNotFound

try:
    from infi.clickhouse_orm import engines, fields  # type: ignore[import-untyped]
except ImportError as e:
    raise ModuleNotFound(extra="clickhouse", packages=["infi-clickhouse-orm"]) from e

from ormy._abc import AbstractABC
from ormy._abc.registry import Registry

from .config import ClickHouseConfig
from .func import get_clickhouse_db
from .models import ClickHouseFieldInfo, ClickHouseModel, ClickHouseQuerySet

# ----------------------- #


class ClickHouseBase(AbstractABC):
    """ClickHouse base class"""

    config: ClassVar[ClickHouseConfig] = ClickHouseConfig()
    engine: ClassVar[Optional[Any]] = None
    _model: ClassVar[Optional[Any]] = None  # type: ignore[assignment]

    __discriminator__ = ["database", "table"]

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)
        cls.__construct_model()

    # ....................... #

    @classmethod
    def set_database(cls):
        """Set ClickHouse database"""

        cls._model.set_database(cls, cls._get_adatabase())  # type: ignore

    # ....................... #

    @classmethod
    def __construct_model(cls):
        """Construct ClickHouse model"""

        _dict_: dict[str, Any] = {}
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
            lambda: cls.config.table,  # type: ignore
        )

    # ....................... #

    @classmethod
    def ch(cls, field: str):
        """
        Get ClickHouse field from infi.clickhouse_orm model

        Args:
            field (str): Field name

        Returns:
            res (Any): infi.clickhouse_orm field
        """

        if cls._model is not None and field in cls._model._fields:
            return getattr(cls._model, field)

        raise InternalError(f"Field `{field}` not found in `{cls.__name__}`")

    # ....................... #

    @classmethod
    def full_table_name(cls):
        """Get full table name"""

        return f"{cls.config.database}.{cls.config.table}"

    # ....................... #

    @classmethod
    def table_name(cls):
        """Get table name"""

        return cls.config.table

    # ....................... #

    @classmethod
    def _get_adatabase(cls):
        """
        Get ClickHouse database connection
        """

        username = (
            cls.config.credentials.username.get_secret_value()
            if cls.config.credentials.username
            else None
        )
        password = (
            cls.config.credentials.password.get_secret_value()
            if cls.config.credentials.password
            else None
        )

        return get_clickhouse_db(
            db_name=cls.config.database,
            username=username,
            password=password,
            db_url=cls.config.url(),
        )

    # ....................... #

    @classmethod
    def objects(cls) -> ClickHouseQuerySet:
        """Get ClickHouse query set"""

        return cls._model.objects_in(cls._get_adatabase())  # type: ignore

    # ....................... #

    @classmethod
    def _get_materialized_fields(cls):
        """Get materialized fields"""

        fields = []

        for x, v in cls.model_fields.items():
            if v.clickhouse.materialized:  # type: ignore[attr-defined]
                fields.append(x)

        return fields

    # ....................... #

    @classmethod
    def insert(cls, records: Self | list[Self], batch_size: int = 1000):
        """
        Insert records into ClickHouse

        Args:
            records (ClickHouseSingleBase | list[ClickHouseSingleBase]): Records to insert
            batch_size (int): Batch size
        """

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

        return cls._get_adatabase().insert(
            model_instances=model_records,
            batch_size=batch_size,
        )

    # ....................... #

    @classmethod
    async def ainsert(cls, records: Self | list[Self], batch_size: int = 1000):
        """
        Insert records into ClickHouse asynchronously

        Args:
            records (ClickHouseSingleBase | list[ClickHouseSingleBase]): Records to insert
            batch_size (int): Batch size
        """

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

        return await cls._get_adatabase().ainsert(
            model_instances=model_records,
            batch_size=batch_size,
        )

    # ....................... #

    @staticmethod
    def registry_helper_set_databases():
        """Set databases for all defined ClickHouse models"""

        entries: list[ClickHouseBase] = Registry.get_by_config(ClickHouseConfig)

        for x in entries:
            x.set_database()
