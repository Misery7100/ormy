from datetime import date, datetime
from enum import Enum
from typing import ClassVar, List, Optional, Type, TypeVar, Union, get_args, get_origin
from uuid import UUID

import backoff
from google.cloud import bigquery
from google.cloud.exceptions import BadRequest, GoogleCloudError, NotFound
from pydantic import BaseModel, Field
from pydantic.fields import ComputedFieldInfo, FieldInfo

from ormy.base.abc import AbstractABC
from ormy.base.func import hex_uuid4, utcnow
from ormy.utils.logging import LogLevel, console_logger

from .config import BigQueryConfig
from .exceptions import BigQueryBackendInsertError, BigQueryInsertError

# ----------------------- #

Bq = TypeVar("Bq", bound="BigQueryBase")
logger = console_logger(__name__, level=LogLevel.INFO)

# ....................... #


class BigQueryBase(AbstractABC):

    # Default fields
    insert_id: str = Field(
        default_factory=hex_uuid4,
        title="Insert ID",
    )
    inserted_at: int = Field(
        default_factory=utcnow,
        title="Inserted At",
    )

    # ....................... #

    configs = [BigQueryConfig()]
    _registry = {BigQueryConfig: {}}

    __PARTITION_FIELD__: ClassVar[Optional[str]] = None
    __CLUSTERING_FIELDS__: ClassVar[List[str]] = []

    # ....................... #

    def __init_subclass__(cls: Type[Bq], **kwargs):
        super().__init_subclass__(**kwargs)
        cls._merge_registry()

        BigQueryBase._registry = cls._merge_registry_helper(
            BigQueryBase._registry,
            cls._registry,
        )

    # ....................... #

    @classmethod
    def _get_dataset(cls: Type[Bq]):
        """
        ...
        """

        cfg = cls.get_config(type_=BigQueryConfig)
        client = cfg.client()

        if client is None:
            raise RuntimeError("BigQuery client is not available")

        try:
            return client.get_dataset(
                dataset_ref=cfg.full_dataset_path,
                timeout=cfg.timeout,
            )

        except NotFound:
            raise ValueError(f"Dataset {cfg.full_dataset_path} not found")

    # ....................... #

    @classmethod
    def _get_table(cls: Type[Bq]):
        """
        ...
        """

        cfg = cls.get_config(type_=BigQueryConfig)
        client = cfg.client()

        if client is None:
            raise RuntimeError("BigQuery client is not available")

        try:
            table = bigquery.Table(cfg.full_table_path)
            return client.get_table(table, timeout=cfg.timeout)

        except NotFound:
            raise ValueError(f"Table {cfg.full_table_path} not found")

    # ....................... #

    @classmethod
    def _get_schema_field_type(
        cls: Type[Bq],
        field: FieldInfo | ComputedFieldInfo,
    ) -> bigquery.enums.SqlTypeNames:
        """
        ...
        """

        if isinstance(field, FieldInfo):
            annot = field.annotation

        else:
            annot = field.return_type

        origin = get_origin(annot)

        if origin is None:
            type_ = annot

        else:
            if isinstance(origin, dict):  #! ???
                return bigquery.enums.SqlTypeNames.STRUCT

            elif origin is Union:
                args = list(get_args(annot))
                args = [x for x in args if x]
                type_ = args[0]

            else:
                type_ = get_args(annot)[0]

        if type_ is not None and issubclass(type_, int):
            return bigquery.enums.SqlTypeNames.INTEGER

        if type_ is not None and issubclass(type_, float):
            return bigquery.enums.SqlTypeNames.FLOAT

        if type_ is not None and issubclass(type_, (str, UUID, Enum)):
            return bigquery.enums.SqlTypeNames.STRING

        if type_ is not None and issubclass(type_, bool):
            return bigquery.enums.SqlTypeNames.BOOLEAN

        if type_ is not None and issubclass(type_, date):
            return bigquery.enums.SqlTypeNames.DATE

        if type_ is not None and issubclass(type_, datetime):
            return bigquery.enums.SqlTypeNames.TIMESTAMP

        if type_ is not None and issubclass(type_, BaseModel):
            return bigquery.enums.SqlTypeNames.RECORD

        raise NotImplementedError(f"Unknown type: {type_}")

    # ....................... #

    @classmethod
    def _get_schema_field_mode(
        cls: Type[Bq],
        field: FieldInfo | ComputedFieldInfo,
    ):
        """
        ...
        """

        if isinstance(field, FieldInfo):
            annot = field.annotation

        else:
            annot = field.return_type

        origin = get_origin(annot)

        if origin is None:
            return "REQUIRED"

        else:
            if isinstance(origin, dict):  #! ???
                return "REQUIRED"

            elif origin is Union:
                args = get_args(annot)

                if type(None) in args and type(list) not in args:
                    return "NULLABLE"

                elif type(list) in args:
                    return "REPEATED"

                else:
                    return "REQUIRED"

            else:
                return "REQUIRED"

    # ....................... #

    @classmethod
    def _get_schema_inner_fields(
        cls: Type[Bq],
        field: FieldInfo | ComputedFieldInfo,
    ):
        """
        ...
        """

        if isinstance(field, FieldInfo):
            annot = field.annotation

        else:
            annot = field.return_type

        origin = get_origin(annot)

        if origin is None:
            type_ = annot

        else:
            if isinstance(origin, dict):
                return []

            elif origin is Union:
                args = list(get_args(annot))
                args = [x for x in args if x]
                type_ = args[0]

            else:
                type_ = get_args(annot)[0]

        if type_ is not None and issubclass(type_, BaseModel):
            return [cls._get_schema_field(k, v) for k, v in type_.model_fields.items()]

        return []

    # ....................... #

    @classmethod
    def _get_schema_field(
        cls: Type[Bq],
        name: str,
        field: FieldInfo | ComputedFieldInfo,
    ):
        """
        ...
        """

        schema_type = cls._get_schema_field_type(field)
        schema_mode = cls._get_schema_field_mode(field)
        inner_fields = cls._get_schema_inner_fields(field)

        return bigquery.SchemaField(
            name=name,
            field_type=str(schema_type.value),
            mode=schema_mode,
            fields=inner_fields,
        )

    # ....................... #

    @classmethod
    def _get_bq_schema(cls: Type[Bq]):
        """
        ...
        """

        model_fields = list(cls.model_fields.items())
        computed_fields = list(cls.model_computed_fields.items())
        all_fields = model_fields + computed_fields

        return [cls._get_schema_field(k, v) for k, v in all_fields]

    # ....................... #

    @classmethod
    def _create_table(cls: Type[Bq], exists_ok: bool = True):
        """
        ...
        """

        cfg = cls.get_config(type_=BigQueryConfig)
        client = cfg.client()

        if client is None:
            raise RuntimeError("BigQuery client is not available")

        schema = cls._get_bq_schema()

        try:
            table = bigquery.Table(cfg.full_table_path, schema=schema)

            if cls.__PARTITION_FIELD__:
                table.time_partitioning = bigquery.TimePartitioning(
                    field=cls.__PARTITION_FIELD__
                )
                table.require_partition_filter = True

            if cls.__CLUSTERING_FIELDS__:
                table.clustering_fields = cls.__CLUSTERING_FIELDS__

            table = client.create_table(table, timeout=cfg.timeout, exists_ok=exists_ok)

            return table

        except BadRequest:
            raise ValueError(f"Table {cfg.full_table_path} already exists")

        except GoogleCloudError:
            raise ValueError(f"Table {cfg.full_table_path} creation failed")

    # ....................... #

    def bigquery_dump(self, *args, **kwargs):
        kwargs.pop("mode", None)

        return self.model_dump(*args, mode="json", **kwargs)

    # ....................... #

    @classmethod
    @backoff.on_exception(
        backoff.expo,
        exception=BigQueryBackendInsertError,
        max_tries=10,
        jitter=None,
    )
    def insert(cls: Type[Bq], data: List[Bq] | Bq):
        """
        ...
        """

        cfg = cls.get_config(type_=BigQueryConfig)
        client = cfg.client()

        if client is None:
            raise RuntimeError("BigQuery client is not available")

        table = cls._get_table()

        if not isinstance(data, list):
            data = [data]

        records = [x.bigquery_dump() for x in data]
        batches = [
            records[i : i + cfg.max_batch_size]
            for i in range(0, len(records), cfg.max_batch_size)
        ]

        for b in batches:
            try:
                errors = client.insert_rows_json(table, b)

                if errors:
                    err = str(errors[0])
                    if "backendError" in err:
                        raise BigQueryBackendInsertError(
                            "Streaming insert error [temporary]"
                        )

                    raise BigQueryInsertError(errors)

            except (BadRequest, GoogleCloudError) as e:
                if (
                    "Your client has issued a malformed or illegal request."
                    in e.response.text
                    or "Request payload size exceeds the limit: 10485760 bytes."
                    in e.response.text
                    or "Your client issued a request that was too large"
                    in e.response.text
                ):

                    # Use bisect to reduce payload size
                    half_size = len(b) // 2

                    # Recursive end condition
                    if half_size == 0:
                        raise BigQueryInsertError("Row is too large") from e

                    # Recursive call
                    data_batch_1, data_batch_2 = b[:half_size], b[half_size:]
                    cls.insert(data_batch_1)
                    cls.insert(data_batch_2)

                else:
                    raise e

    # ....................... #

    def save(self: Bq):
        """
        ...
        """

        return self.insert(data=self)
