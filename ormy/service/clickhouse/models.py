from math import ceil
from typing import Any, List

from infi.clickhouse_orm import (  # type: ignore[import-untyped]
    database,
    fields,
    models,
    query,
    utils,
)
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from ormy.base.generic import TabularData

from .database import AsyncDatabase

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
    _database: AsyncDatabase

    # ....................... #

    def tabular(self) -> TabularData:
        qs = [r.to_dict(field_names=self._fields) for r in self]

        return TabularData(qs)

    # ....................... #

    async def acount(self):
        """
        Returns the number of matching model instances.
        """

        if self._distinct or self._limits:
            # Use a subquery, since a simple count won't be accurate
            sql = "SELECT count() FROM (%s)" % self.as_sql()
            raw = await self._database.araw(sql)
            return int(raw) if raw else 0

        # Simple case
        conditions = (self._where_q & self._prewhere_q).to_sql(self._model_cls)

        return await self._database.acount(
            self._model_cls,
            conditions,
        )

    # ....................... #

    async def __apaginate(self, page_num=1, page_size=100):
        """
        Returns a single page of model instances that match the queryset.
        Note that `order_by` should be used first, to ensure a correct
        partitioning of records into pages.

        - `page_num`: the page number (1-based), or -1 to get the last page.
        - `page_size`: number of records to return per page.

        The result is a namedtuple containing `objects` (list), `number_of_objects`,
        `pages_total`, `number` (of the current page), and `page_size`.
        """

        count = await self.acount()
        pages_total = int(ceil(count / float(page_size)))
        if page_num == -1:
            page_num = pages_total
        elif page_num < 1:
            raise ValueError("Invalid page number: %d" % page_num)
        offset = (page_num - 1) * page_size

        return database.Page(
            objects=list(self[offset : offset + page_size]),
            number_of_objects=count,
            pages_total=pages_total,
            number=page_num,
            page_size=page_size,
        )

    # ....................... #

    async def apaginate(self, page_num: int = 1, page_size: int = 100):
        p = await self.__apaginate(
            page_num=page_num,
            page_size=page_size,
        )

        return ClickHousePage.from_infi_page(
            model_cls=self._model_cls,
            fields=self._fields,
            p=p,
        )

    # ....................... #

    def paginate(self, page_num: int = 1, page_size: int = 100):
        p = super().paginate(
            page_num=page_num,
            page_size=page_size,
        )

        return ClickHousePage.from_infi_page(
            model_cls=self._model_cls,
            fields=self._fields,
            p=p,
        )

    # ....................... #

    def aggregate(self, *args, **kwargs):
        return ClickHouseAggregateQuerySet(self, args, kwargs)

    # ....................... #

    async def adelete(self):
        """
        Deletes all records matched by this queryset's conditions.
        Note that ClickHouse performs deletions in the background, so they are not immediate.
        """

        self._verify_mutation_allowed()
        conditions = (self._where_q & self._prewhere_q).to_sql(self._model_cls)
        sql = "ALTER TABLE $db.`%s` DELETE WHERE %s" % (
            self._model_cls.table_name(),
            conditions,
        )
        await self._database.araw(sql)

        return self

    # ....................... #

    async def aupdate(self, **kwargs):
        """
        Updates all records matched by this queryset's conditions.
        Keyword arguments specify the field names and expressions to use for the update.
        Note that ClickHouse performs updates in the background, so they are not immediate.
        """

        assert kwargs, "No fields specified for update"

        self._verify_mutation_allowed()
        fields = utils.comma_join(
            "`%s` = %s" % (name, utils.arg_to_sql(expr))
            for name, expr in kwargs.items()
        )
        conditions = (self._where_q & self._prewhere_q).to_sql(self._model_cls)
        sql = "ALTER TABLE $db.`%s` UPDATE %s WHERE %s" % (
            self._model_cls.table_name(),
            fields,
            conditions,
        )
        await self._database.araw(sql)

        return self


# ....................... #


class ClickHouseAggregateQuerySet(ClickHouseQuerySet, query.AggregateQuerySet):
    def tabular(self) -> TabularData:
        all_fields = list(self._fields) + list(self._calculated_fields.keys())
        qs = [r.to_dict(field_names=all_fields) for r in self]

        return TabularData(qs)

    # ....................... #

    def paginate(self, page_num: int = 1, page_size: int = 100):
        p = super(query.AggregateQuerySet, self).paginate(
            page_num=page_num, page_size=page_size
        )
        all_fields = list(self._fields) + list(self._calculated_fields.keys())

        return ClickHousePage.from_infi_page(
            model_cls=self._model_cls,
            fields=all_fields,
            p=p,
        )

    # ....................... #

    async def apaginate(self, page_num: int = 1, page_size: int = 100):
        p = await self.__apaginate(
            page_num=page_num,
            page_size=page_size,
        )
        all_fields = list(self._fields) + list(self._calculated_fields.keys())

        return ClickHousePage.from_infi_page(
            model_cls=self._model_cls,
            fields=all_fields,
            p=p,
        )

    # ....................... #

    def only(self, *field_names):
        """
        This method is not supported on `AggregateQuerySet`.
        """
        raise NotImplementedError('Cannot use "only" with AggregateQuerySet')

    # ....................... #

    def aggregate(self, *args, **kwargs):
        """
        This method is not supported on `AggregateQuerySet`.
        """

        raise NotImplementedError("Cannot re-aggregate an AggregateQuerySet")

    # ....................... #

    def count(self):
        """
        Returns the number of rows after aggregation.
        """

        return super(query.AggregateQuerySet, self).count()

    # ....................... #

    async def acount(self):
        """
        Returns the number of rows after aggregation.
        """

        sql = "SELECT count() FROM (%s)" % self.as_sql()
        raw = await self._database.araw(sql)

        return int(raw) if raw else 0


# ....................... #


# TODO: async methods
class ClickHouseModel(models.Model):
    @classmethod
    def objects_in(cls, database):
        return ClickHouseQuerySet(cls, database)
