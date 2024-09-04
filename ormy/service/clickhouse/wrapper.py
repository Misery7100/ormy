import inspect
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Type,
    TypeVar,
)

from infi.clickhouse_orm import (  # type: ignore[import-untyped]
    database,
    engines,
    fields,
    models,
    query,
)
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from ormy.base.abc import AbstractABC
from ormy.utils.logging import LogLevel, console_logger

from .config import ClickHouseConfig
from .func import get_clickhouse_db

# ----------------------- #

Ch = TypeVar("Ch", bound="ClickHouseBase")
logger = console_logger(__name__, level=LogLevel.INFO)

# ....................... #


class ClickHouseFieldInfo(FieldInfo):
    def __init__(
        self,
        default: Any,
        *,
        clickhouse: fields.Field,
        **kwargs,
    ):
        super().__init__(default=default, **kwargs)

        self.clickhouse = clickhouse


# ....................... #


def ClickHouseField(
    default: Any = PydanticUndefined,
    *,
    clickhouse: fields.Field = fields.StringField(),
    **kwargs: Any,
) -> ClickHouseFieldInfo:
    return ClickHouseFieldInfo(default, clickhouse=clickhouse, **kwargs)


# ....................... #


class ClickHousePage:
    def __init__(
        self,
        model_cls: "ClickHouseModel",
        fields: List[str],
        objects: List["ClickHouseModel"],
        number_of_objects: int,
        pages_total: int,
        number: int,
        page_size: int,
    ):

        self._model_cls = model_cls
        self.fields = fields
        self.objects = objects
        self.number_of_objects = number_of_objects
        self.pages_total = pages_total
        self.number = number
        self.page_size = page_size

    # ....................... #

    @classmethod
    def from_infi_page(
        cls,
        model_cls: "ClickHouseModel",
        fields: List[str],
        p: database.Page,
    ):
        return cls(
            model_cls=model_cls,
            fields=fields,
            objects=p.objects,
            number_of_objects=p.number_of_objects,
            pages_total=p.pages_total,
            number=p.number,
            page_size=p.page_size,
        )

    # ....................... #

    def evaluate(self, raw: bool = False):
        qs = [r.to_dict(field_names=self.fields) for r in self.objects]

        if raw:
            return qs

        return [self._model_cls._ref.model_validate_universal(r) for r in qs]


# ....................... #


class ClickHouseQuerySet(query.QuerySet):
    def evaluate(self, raw: bool = False):
        qs = [r.to_dict(field_names=self._fields) for r in self]

        if raw:
            return qs

        if set(self._fields) != set(self._model_cls._ref.model_fields.keys()):
            raise ValueError(
                "Cannot validate incomplete model fields. Try use `raw=True`"
            )

        return [self._model_cls._ref.model_validate_universal(r) for r in qs]

    # ....................... #

    def paginate(self, page_num: int = 1, page_size: int = 100):
        p = super().paginate(page_num=page_num, page_size=page_size)

        return ClickHousePage.from_infi_page(
            model_cls=self._model_cls,
            fields=self._fields,
            p=p,
        )

    # ....................... #

    def aggregate(self, *args, **kwargs):
        return ClickHouseAggregateQuerySet(self * args, **kwargs)


# ....................... #


class ClickHouseAggregateQuerySet(query.AggregateQuerySet):
    def evaluate(self, raw: bool = False):
        return [r.to_dict(field_names=self._fields) for r in self]


# ....................... #


class ClickHouseModel(models.Model):
    _ref: ClassVar["ClickHouseBase"]

    # ....................... #

    @classmethod
    def objects_in(cls, database):
        return ClickHouseQuerySet(cls, database)


# ....................... #


class ClickHouseBase(AbstractABC):

    configs = [ClickHouseConfig()]
    engine: ClassVar[engines.Engine] = None

    _registry = {ClickHouseConfig: {}}
    _model: ClassVar[ClickHouseModel] = None  # type: ignore[assignment]

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
        _dict_: Dict[str, ClickHouseFieldInfo | fields.Field] = {}
        orm_fields = {}
        engine = None

        parents = inspect.getmro(cls)

        for p in parents[::-1]:
            if issubclass(p, ClickHouseBase):
                _dict_.update(p.__dict__)

        for attr_name, attr_value in _dict_.items():
            if isinstance(attr_value, ClickHouseFieldInfo):
                orm_fields[attr_name] = attr_value.clickhouse

            elif isinstance(attr_value, fields.Field):
                orm_fields[attr_name] = attr_value

            elif isinstance(attr_value, engines.Engine):
                engine = attr_value

        # Dynamically create the ORM model
        orm_attrs = {"engine": engine, **orm_fields, "_ref": cls}

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
            db_url=cfg.db_url(),
        )

    # ....................... #

    @classmethod
    def objects(cls: Type[Ch]) -> ClickHouseQuerySet:
        return cls._model.objects_in(cls._get_adatabase())

    # ....................... #

    @classmethod
    def evaluate(
        cls: Type[Ch],
        qs: ClickHouseQuerySet | ClickHousePage,
        raw: bool = False,
    ) -> List[Ch | Dict[str, Any]]:
        if isinstance(qs, ClickHousePage):
            qss = [r.to_dict() for r in qs.objects]  # type: ignore

        else:
            qss = [r.to_dict(field_names=qs._fields) for r in qs]  # type: ignore

        if raw:
            return qss

        return [cls.model_validate_universal(r) for r in qss]  # type: ignore

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
                    exclude=cls._get_materialized_fields(),
                )
            )
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
                    exclude=cls._get_materialized_fields(),
                )
            )
            for record in records
        ]
        return await cls._get_adatabase().ainsert(model_records, batch_size=batch_size)

    # ....................... #
