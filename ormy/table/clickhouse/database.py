from math import ceil
from typing import Any

from ormy.exceptions import ModuleNotFound

try:
    import httpx
    from infi.clickhouse_orm import (  # type: ignore[import-untyped]
        database,
        models,
        utils,
    )
except ImportError as e:
    raise ModuleNotFound(
        extra="clickhouse", packages=["httpx", "infi-clickhouse-orm"]
    ) from e

# ----------------------- #


class AsyncDatabase(database.Database):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.__username = kwargs.get("username", "default")
        self.__password = kwargs.get("password", None)

    # ....................... #

    async def _asend_stream(self, data, settings: Any = None):
        if isinstance(data, str):
            data = data.encode("utf-8")

            if self.log_statements:
                pass  # TODO: fix

        params = self._build_params(settings)

        async with httpx.AsyncClient(
            auth=(self.__username, self.__password)
        ) as session:
            async with session.stream(
                method="POST",
                url=self.db_url,
                params=params,
                content=data,
                timeout=self.timeout,
            ) as r:
                if r.status_code != 200:
                    raise database.ServerError(r.text)

                async for line in r.aiter_lines():
                    yield line

    # ....................... #

    async def _asend(self, data, settings: Any = None):
        if isinstance(data, str):
            data = data.encode("utf-8")

            if self.log_statements:
                pass  # TODO: fix

        params = self._build_params(settings)

        async with httpx.AsyncClient(
            auth=(self.__username, self.__password)
        ) as session:
            r = await session.post(
                url=self.db_url,
                params=params,
                content=data,
                timeout=self.timeout,
            )

        if r.status_code != 200:
            raise database.ServerError(r.text)

        return r

    # ....................... #

    async def aselect(self, query, model_class=None, settings=None):
        """
        Performs a query and returns a generator of model instances.

        - `query`: the SQL query to execute.
        - `model_class`: the model class matching the query's table,
          or `None` for getting back instances of an ad-hoc model.
        - `settings`: query settings to send as HTTP GET parameters
        """
        query += " FORMAT TabSeparatedWithNamesAndTypes"
        query = self._substitute(query, model_class)
        lines = self._asend_stream(query, settings)

        field_names = utils.parse_tsv(await anext(lines))  # noqa: F821
        field_types = utils.parse_tsv(await anext(lines))  # noqa: F821
        model_class = model_class or models.ModelBase.create_ad_hoc_model(
            zip(field_names, field_types)
        )
        async for line in lines:
            # skip blank line left by WITH TOTALS modifier
            if line:
                yield model_class.from_tsv(  # type: ignore
                    line, field_names, self.server_timezone, self
                )

    # ....................... #

    async def ainsert(self, model_instances, batch_size: int = 1000):
        """
        Insert records into the database.

        - `model_instances`: any iterable containing instances of a single model class.
        - `batch_size`: number of records to send per chunk (use a lower number if your records are very large).
        """
        from io import BytesIO

        i = iter(model_instances)
        try:
            first_instance = next(i)
        except StopIteration:
            return  # model_instances is empty
        model_class = first_instance.__class__

        if first_instance.is_read_only() or first_instance.is_system_model():
            raise database.DatabaseException(
                "You can't insert into read only and system tables"
            )

        fields_list = ",".join(
            ["`%s`" % name for name in first_instance.fields(writable=True)]
        )
        fmt = "TSKV" if model_class.has_funcs_as_defaults() else "TabSeparated"
        query = "INSERT INTO $table (%s) FORMAT %s\n" % (fields_list, fmt)

        async def agen():
            buf = BytesIO()
            buf.write(self._substitute(query, model_class).encode("utf-8"))
            first_instance.set_database(self)
            buf.write(first_instance.to_db_string())
            # Collect lines in batches of batch_size
            lines = 2

            for instance in i:
                instance.set_database(self)
                buf.write(instance.to_db_string())
                lines += 1

                if lines >= batch_size:
                    # Return the current batch of lines
                    yield buf.getvalue()
                    # Start a new batch
                    buf = BytesIO()
                    lines = 0
            # Return any remaining lines in partial batch
            if lines:
                yield buf.getvalue()

        await self._asend(agen())

    # ....................... #

    async def acount(self, model_class, conditions=None):
        """
        Counts the number of records in the model's table.

        - `model_class`: the model to count.
        - `conditions`: optional SQL conditions (contents of the WHERE clause).
        """
        from infi.clickhouse_orm.query import Q  # type: ignore[import-untyped]

        query = "SELECT count() FROM $table"

        if conditions:
            if isinstance(conditions, Q):
                conditions = conditions.to_sql(model_class)
            query += " WHERE " + str(conditions)

        query = self._substitute(query, model_class)

        r = await self._asend(query)

        return int(r.text) if r.text else 0

    # ....................... #

    async def araw(self, query, settings=None):
        """
        Performs a query and returns its output as text.

        - `query`: the SQL query to execute.
        - `settings`: query settings to send as HTTP GET parameters
        - `stream`: if true, the HTTP response from ClickHouse will be streamed.
        """
        query = self._substitute(query, None)

        return (await self._asend(query, settings=settings)).text

    # ....................... #

    async def apaginate(
        self,
        model_class,
        order_by,
        page_num=1,
        page_size=100,
        conditions=None,
        settings=None,
    ):
        """
        Selects records and returns a single page of model instances.

        - `model_class`: the model class matching the query's table,
          or `None` for getting back instances of an ad-hoc model.
        - `order_by`: columns to use for sorting the query (contents of the ORDER BY clause).
        - `page_num`: the page number (1-based), or -1 to get the last page.
        - `page_size`: number of records to return per page.
        - `conditions`: optional SQL conditions (contents of the WHERE clause).
        - `settings`: query settings to send as HTTP GET parameters

        The result is a namedtuple containing `objects` (list), `number_of_objects`,
        `pages_total`, `number` (of the current page), and `page_size`.
        """
        from infi.clickhouse_orm.query import Q

        count = await self.acount(model_class, conditions)
        pages_total = int(ceil(count / float(page_size)))

        if page_num == -1:
            page_num = max(pages_total, 1)

        elif page_num < 1:
            raise ValueError("Invalid page number: %d" % page_num)

        offset = (page_num - 1) * page_size
        query = "SELECT {} FROM $table".format(", ".join(model_class.fields().keys()))

        if conditions:
            if isinstance(conditions, Q):
                conditions = conditions.to_sql(model_class)
            query += " WHERE " + str(conditions)

        query += " ORDER BY %s" % order_by
        query += " LIMIT %d, %d" % (offset, page_size)

        query = self._substitute(query, model_class)

        return database.Page(
            objects=(
                list(await self.aselect(query, model_class, settings)) if count else []  # type: ignore
            ),
            number_of_objects=count,
            pages_total=pages_total,
            number=page_num,
            page_size=page_size,
        )
