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

from infi.clickhouse_orm import (  # type: ignore[import-untyped]
    engines,
    fields,
)

from ormy.base.abc import AbstractSingleABC
from ormy.utils.logging import LogLevel, console_logger

from .config import ClickHouseConfig
from .func import get_clickhouse_db
from .models import ClickHouseFieldInfo, ClickHouseModel, ClickHouseQuerySet

# ----------------------- #

Ch = TypeVar("Ch", bound="ClickHouseSingleBase")
logger = console_logger(__name__, level=LogLevel.INFO)

# ----------------------- #


class ClickHouseSingleBase(AbstractSingleABC):
    """ClickHouse base class"""

    config: ClassVar[ClickHouseConfig] = ClickHouseConfig()
    engine: ClassVar[Optional[engines.Engine]] = None

    _registry = {ClickHouseConfig: {}}
    _model: ClassVar[Optional[ClickHouseModel]] = None  # type: ignore[assignment]

    # ....................... #

    def __init_subclass__(cls: type[Ch], **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)

        cls._register_subclass_helper(discriminator=["database", "table"])
        cls.__construct_model()
        cls._merge_registry()

        ClickHouseSingleBase._registry = cls._merge_registry_helper(
            ClickHouseSingleBase._registry,
            cls._registry,
        )

        cls._model.set_database(cls, cls._get_adatabase())  # type: ignore

    # ....................... #

    @classmethod
    def __construct_model(cls: Type[Ch]):
        """Construct ClickHouse model"""

        _dict_: Dict[str, Any] = {}
        orm_fields = {}
        engine = None

        parents = inspect.getmro(cls)

        for p in parents[::-1]:
            if issubclass(p, ClickHouseSingleBase):
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
    def _get_adatabase(cls: Type[Ch]):
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
    def objects(cls: Type[Ch]) -> ClickHouseQuerySet:
        """Get ClickHouse query set"""

        return cls._model.objects_in(cls._get_adatabase())  # type: ignore

    # ....................... #

    @classmethod
    def _get_materialized_fields(cls: Type[Ch]):
        """Get materialized fields"""

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
        """
        Insert records into ClickHouse

        Args:
            records (ClickHouseSingleBase | List[ClickHouseSingleBase]): Records to insert
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
    async def ainsert(
        cls: Type[Ch],
        records: Ch | List[Ch],
        batch_size: int = 1000,
    ) -> None:
        """
        Insert records into ClickHouse asynchronously

        Args:
            records (ClickHouseSingleBase | List[ClickHouseSingleBase]): Records to insert
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
