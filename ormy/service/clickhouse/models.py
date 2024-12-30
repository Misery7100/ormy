from typing import Any, List

from infi.clickhouse_orm import (  # type: ignore[import-untyped]
    database,
    fields,
    models,
    query,
)
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from ormy.base.generic import TabularData

# ----------------------- #


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

    def tabular(self) -> TabularData:
        qs = [r.to_dict(field_names=self.fields) for r in self.objects]

        return TabularData(qs)


# ....................... #


class ClickHouseQuerySet(query.QuerySet):
    def tabular(self) -> TabularData:
        qs = [r.to_dict(field_names=self._fields) for r in self]

        return TabularData(qs)

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
        return ClickHouseAggregateQuerySet(self, args, kwargs)


# ....................... #


class ClickHouseAggregateQuerySet(query.AggregateQuerySet):
    def tabular(self) -> TabularData:
        all_fields = list(self._fields) + list(self._calculated_fields.keys())
        qs = [r.to_dict(field_names=all_fields) for r in self]

        return TabularData(qs)

    # ....................... #

    def paginate(self, page_num: int = 1, page_size: int = 100):
        p = super().paginate(page_num=page_num, page_size=page_size)
        all_fields = list(self._fields) + list(self._calculated_fields.keys())

        return ClickHousePage.from_infi_page(
            model_cls=self._model_cls,
            fields=all_fields,
            p=p,
        )


# ....................... #


class ClickHouseModel(models.Model):
    @classmethod
    def objects_in(cls, database):
        return ClickHouseQuerySet(cls, database)
