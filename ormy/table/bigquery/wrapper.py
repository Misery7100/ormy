from datetime import date, datetime
from enum import Enum
from typing import Any, ClassVar, Optional, Self, Union, get_args, get_origin
from uuid import UUID

from pydantic import BaseModel
from pydantic.fields import ComputedFieldInfo, FieldInfo

from ormy.exceptions import Conflict, InternalError, ModuleNotFound, NotFound

try:
    from google.cloud import bigquery, exceptions
except ImportError as e:
    raise ModuleNotFound(extra="bigquery", packages=["google-cloud-bigquery"]) from e

from ormy.table._abc import TableABC

from .config import BigQueryConfig
from .exceptions import BigQueryBackendInsertError, BigQueryInsertError

# ----------------------- #


class BigQueryBase(TableABC):
    """BigQuery base class"""

    config: ClassVar[BigQueryConfig] = BigQueryConfig()

    __PARTITION_FIELD__: ClassVar[Optional[str]] = None
    __CLUSTERING_FIELDS__: ClassVar[list[str]] = []

    __discriminator__: ClassVar[list[str]] = ["dataset", "table"]

    # ....................... #

    @classmethod
    def _get_dataset(cls):
        """Get BigQuery dataset"""

        client = cls.config.client()

        if client is None:
            raise InternalError("BigQuery client is not available")

        try:
            return client.get_dataset(
                dataset_ref=cls.config.full_dataset_path,
                timeout=cls.config.timeout,
            )

        except exceptions.NotFound:
            raise NotFound(f"Dataset {cls.config.full_dataset_path} not found")

        except Exception:
            raise InternalError("Failed to get BigQuery dataset")

    # ....................... #

    @classmethod
    def _get_table(cls):
        """Get BigQuery table"""

        client = cls.config.client()

        if client is None:
            raise InternalError("BigQuery client is not available")

        try:
            table = bigquery.Table(table_ref=cls.config.full_table_path)
            return client.get_table(
                table=table,
                timeout=cls.config.timeout,
            )

        except exceptions.NotFound:
            raise NotFound(f"Table {cls.config.full_table_path} not found")

        except Exception:
            raise InternalError("Failed to get BigQuery table")

    # ....................... #

    @classmethod
    def __get_schema_field_type(cls, field: FieldInfo | ComputedFieldInfo):
        """
        Get BigQuery schema field type

        Args:
            field (pydantic.fields.FieldInfo | pydantic.fields.ComputedFieldInfo): Field to get type of

        Returns:
            type_ (google.cloud.bigquery.enums.SqlTypeNames): BigQuery schema field type
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

        if type_ is not None and issubclass(type_, bool):
            return bigquery.enums.SqlTypeNames.BOOLEAN

        if type_ is not None and issubclass(type_, int):
            return bigquery.enums.SqlTypeNames.INTEGER

        if type_ is not None and issubclass(type_, float):
            return bigquery.enums.SqlTypeNames.FLOAT

        if type_ is not None and issubclass(type_, (str, UUID, Enum)):
            return bigquery.enums.SqlTypeNames.STRING

        if type_ is not None and issubclass(type_, date):
            return bigquery.enums.SqlTypeNames.DATE

        if type_ is not None and issubclass(type_, datetime):
            return bigquery.enums.SqlTypeNames.TIMESTAMP

        if type_ is not None and issubclass(type_, BaseModel):
            return bigquery.enums.SqlTypeNames.RECORD

        raise InternalError(f"Unknown type: {type_}")

    # ....................... #

    @classmethod
    def __get_schema_field_mode(cls, field: FieldInfo | ComputedFieldInfo):
        """
        Get BigQuery schema field mode

        Args:
            field (pydantic.fields.FieldInfo | pydantic.fields.ComputedFieldInfo): Field to get mode of

        Returns:
            mode (str): BigQuery schema field mode
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
    def __get_schema_field(cls, name: str, field: FieldInfo | ComputedFieldInfo):
        """
        Get BigQuery schema field

        Args:
            name (str): Name of the field
            field (pydantic.fields.FieldInfo | pydantic.fields.ComputedFieldInfo): Field to get schema of

        Returns:
            field (google.cloud.bigquery.SchemaField): BigQuery schema field
        """

        schema_type = cls.__get_schema_field_type(field)
        schema_mode = cls.__get_schema_field_mode(field)
        inner_fields = cls.__get_schema_inner_fields(field)

        return bigquery.SchemaField(
            name=name,
            field_type=str(schema_type.value),
            mode=schema_mode,
            fields=inner_fields,
        )

    # ....................... #

    @classmethod
    def __get_schema_inner_fields(cls, field: FieldInfo | ComputedFieldInfo):
        """
        Get BigQuery schema inner fields

        Args:
            field (pydantic.fields.FieldInfo | pydantic.fields.ComputedFieldInfo): Field to get inner fields of

        Returns:
            inner_fields (list[google.cloud.bigquery.SchemaField]): BigQuery schema inner fields
        """

        fields: list[bigquery.SchemaField] = []

        if isinstance(field, FieldInfo):
            annot = field.annotation

        else:
            annot = field.return_type

        origin = get_origin(annot)

        if origin is None:
            type_ = annot

        else:
            if isinstance(origin, dict):
                return fields

            elif origin is Union:
                args = list(get_args(annot))
                args = [x for x in args if x]
                type_ = args[0]

            else:
                type_ = get_args(annot)[0]

        if type_ is not None and issubclass(type_, BaseModel):
            fields = [
                cls.__get_schema_field(k, v) for k, v in type_.model_fields.items()
            ]

        return fields

    # ....................... #

    @classmethod
    def __get_full_schema(cls):
        """
        Get BigQuery full schema

        Returns:
            full_schema (list[google.cloud.bigquery.SchemaField]): BigQuery full schema
        """

        model_fields = list(cls.model_fields.items())
        computed_fields = list(cls.model_computed_fields.items())
        all_fields = model_fields + computed_fields

        return [cls.__get_schema_field(k, v) for k, v in all_fields]

    # ....................... #

    @classmethod
    def create_table(cls, exists_ok: bool = True):
        """
        Create BigQuery table

        Args:
            exists_ok (bool): Whether to allow existing table
        """

        client = cls.config.client()

        if client is None:
            raise InternalError("BigQuery client is not available")

        schema = cls.__get_full_schema()

        try:
            table = bigquery.Table(
                table_ref=cls.config.full_table_path,
                schema=schema,
            )

            if cls.__PARTITION_FIELD__:
                table.time_partitioning = bigquery.TimePartitioning(
                    field=cls.__PARTITION_FIELD__
                )
                table.require_partition_filter = True

            if cls.__CLUSTERING_FIELDS__:
                table.clustering_fields = cls.__CLUSTERING_FIELDS__

            table = client.create_table(
                table=table,
                timeout=cls.config.timeout,
                exists_ok=exists_ok,
            )

        except exceptions.BadRequest:
            raise Conflict(f"Table {cls.config.full_table_path} already exists")

        except (exceptions.GoogleCloudError, Exception):
            raise InternalError("Failed to create BigQuery table")

    # ....................... #

    # TODO: add backoff ?
    """
    @backoff.on_exception(
        backoff.expo,
        exception=BigQueryBackendInsertError, #! <- Use custom exception
        max_tries=10,
        jitter=None,
    )
    """

    @classmethod
    def insert(cls, data: list[Self] | list[dict[str, Any]] | Self | dict[str, Any]):
        """
        Insert data into BigQuery table
        """

        client = cls.config.client()

        if client is None:
            raise InternalError("BigQuery client is not available")

        table = cls._get_table()

        if not isinstance(data, list):
            data_ = [data]

        else:
            data_ = data  # type: ignore[assignment]

        if not data_:
            raise InternalError("No data to insert")

        records = [
            x if isinstance(x, dict) else x.model_dump(mode="json") for x in data_
        ]
        batches = [
            records[i : i + cls.config.max_batch_size]
            for i in range(0, len(records), cls.config.max_batch_size)
        ]

        for b in batches:
            try:
                errors = client.insert_rows(table=table, rows=b)

                if errors:
                    err = str(errors[0])

                    if "backendError" in err:
                        raise BigQueryBackendInsertError(
                            "Streaming insert error [temporary]"
                        )

                    raise BigQueryInsertError(
                        "Failed to insert data into BigQuery table"
                    )

            except (exceptions.BadRequest, exceptions.GoogleCloudError) as e:
                if (
                    "Your client has issued a malformed or illegal request."
                    in e.response.text  # type: ignore
                    or "Request payload size exceeds the limit: 10485760 bytes."
                    in e.response.text  # type: ignore
                    or "Your client issued a request that was too large"
                    in e.response.text  # type: ignore
                ):

                    # Use bisect to reduce payload size
                    half_size = len(b) // 2

                    # Recursive end condition
                    if half_size == 0:
                        raise BigQueryInsertError(
                            "Failed to insert data into BigQuery table: row is too large"
                        ) from e

                    # Recursive call
                    b1 = b[:half_size]
                    b2 = b[half_size:]
                    cls.insert(b1)
                    cls.insert(b2)

                else:
                    raise BigQueryInsertError(
                        "Failed to insert data into BigQuery table"
                    )

            except Exception:
                raise InternalError("Failed to insert data into BigQuery table")

    # ....................... #

    @classmethod
    def query(cls, query: str, params: dict[str, Any] = {}):
        """
        Query BigQuery table
        """

        raise NotImplementedError("Not implemented")
