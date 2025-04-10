from typing import Annotated, Any, ClassVar, Generic, Optional, Self, TypeVar

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    GetCoreSchemaHandler,
    SecretStr,
    model_validator,
)
from pydantic_core import core_schema

from .decorator import json_schema_modifier, trim_description
from .generic import TabularData

# ----------------------- #

G = TypeVar("G")

# ----------------------- #


class IgnorePlaceholder:
    """Ignore placeholder singleton"""

    _instance = None

    # ....................... #

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    # ....................... #

    def __repr__(self):
        return "<IGNORE>"

    # ....................... #

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source: Any,
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        """
        Define how the schema handles the placeholder during validation and serialization.
        """
        return core_schema.no_info_plain_validator_function(cls.validate)

    # ....................... #

    @staticmethod
    def validate(value: Any) -> Any:
        """
        Validator to handle the placeholder during validation.
        """
        if value is IgnorePlaceholder._instance:
            return IgnorePlaceholder._instance

        return value


IGNORE = IgnorePlaceholder()

# ....................... #


class TypeWithIgnore(Generic[G]):
    def __class_getitem__(cls, item: G) -> Any:
        """
        Dynamically creates a Field instance for Annotated types.
        Overrides default with IGNORE if not explicitly set.
        """

        return Annotated[item | IgnorePlaceholder, Field(default=IGNORE)]


# ....................... #


@json_schema_modifier(trim_description)
class BaseWithIgnore(BaseModel):
    """Base class for all Pydantic models within the package with ignore placeholder"""

    model_config = ConfigDict(ignored_types=(IgnorePlaceholder,))

    # ....................... #

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source: Any,
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        """
        Override the core schema to enforce IGNORE as default for fields.
        """

        def enforce_ignore(schema: core_schema.ModelField) -> core_schema.ModelField:
            schema["schema"]["default"] = IGNORE  # type: ignore

            return schema

        original_schema = super().__get_pydantic_core_schema__(source, handler)

        if isinstance(original_schema, dict):
            schema = original_schema.get("schema", {})

            if schema and "fields" in schema:
                for fname, fschema in schema["fields"].items():  # type: ignore
                    schema["fields"][fname] = enforce_ignore(fschema)  # type: ignore

                original_schema["schema"] = schema  # type: ignore

        return original_schema

    # ....................... #

    def model_dump(self: Self, *args, **kwargs):
        """
        Override the model dump to exclude IGNORE values
        """

        kwargs["exclude_defaults"] = True

        return super().model_dump(*args, **kwargs)

    # ....................... #

    def model_dump_json(self: Self, *args, **kwargs):
        """
        Override the model dump to exclude IGNORE values
        """

        kwargs["exclude_defaults"] = True

        return super().model_dump_json(*args, **kwargs)


# ....................... #


@json_schema_modifier(trim_description)
class Base(BaseModel):
    """Base class for all Pydantic models within the package"""

    model_config = ConfigDict(
        validate_assignment=True,
        validate_default=True,
    )

    specific_fields: ClassVar[dict[str, list[str]]] = {
        "datetime": [
            "created_at",
            "last_update_at",
            "deadline",
            "timestamp",
        ]
    }
    # ....................... #

    @staticmethod
    def _parse_json_schema_defs(json_schema: dict):
        """
        Parse the definitions from a JSON schema

        Args:
            json_schema (dict): The JSON schema to parse

        Returns:
            extracted_defs (dict[str, dict]): The extracted definitions
        """

        defs = json_schema.get("$defs", {})
        extracted_defs: dict[str, dict] = {}

        for k, v in defs.items():
            if "enum" in v:
                v["value"] = v["enum"]
                v["type"] = "array"  # "enum"
                extracted_defs[k] = v

        return extracted_defs

    # ....................... #

    @classmethod
    def model_flat_schema(
        cls,
        include: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
        extra: Optional[list[str]] = None,
        extra_definitions: list[dict[str, str]] = [],
    ):
        """
        Generate a flat schema for the model data structure with extra definitions

        Args:
            include (list[str], optional): The fields to include in the schema. Defaults to None.
            exclude (list[str], optional): The fields to exclude from the schema. Defaults to None.
            extra (list[str], optional): The extra fields to include in the schema. Defaults to None.
            extra_definitions (list[dict[str, str]], optional): The extra definitions to include in the schema. Defaults to [].

        Returns:
            schema (list[dict[str, Any]]): The flat schema for the model
        """

        schema = cls.model_json_schema(mode="serialization")
        defs = cls._parse_json_schema_defs(schema)
        keys: list[str] = [k for k, _ in schema["properties"].items()]
        flat_schema: list[dict[str, Any]] = []
        schema_keys = ["key", "type", "value"]

        if include is not None:
            keys = include

        elif exclude is not None:
            keys = [k for k in keys if k not in exclude]

        for k, v in schema["properties"].items():
            if k not in keys:
                continue

            type_ = v.get("type", "string")

            # skip array of references
            if type_ == "array":
                if items := v.get("items", {}):
                    if "$ref" in items.keys():
                        continue

            if ref := v.get("$ref", None):
                ref_name = ref.split("/")[-1]

                if r := defs.get(ref_name, {}):
                    data = {"key": k, **r}
                    data = {
                        k: v
                        for k, v in data.items()
                        if k in schema_keys and v is not None
                    }
                    flat_schema.append(data)

            # check for reference
            if refs := v.get("allOf", []):
                if len(refs) > 1:
                    continue

                ref_name = refs[0]["$ref"].split("/")[-1]

                # parse definitions, include only first level references
                if ref := defs.get(ref_name, {}):
                    data = {"key": k, **ref}
                    data["title"] = v.get("title", data.get("title", k.title()))
                    data = {
                        k: v
                        for k, v in data.items()
                        if k in schema_keys and v is not None
                    }
                    flat_schema.append(data)

            # include not referenced fields
            else:
                data = {"key": k, **v}
                data = {
                    k: v for k, v in data.items() if k in schema_keys and v is not None
                }
                flat_schema.append(data)

        # include extra based on extra definitions list
        if extra and extra_definitions:
            for ef in extra:
                if exdef := next(
                    (x for x in extra_definitions if x["key"] == ef), None
                ):
                    flat_schema.append(exdef)

        # follow up type definition from specific fields
        for field in flat_schema:
            field["type"] = cls._define_dtype(field["key"], field.get("type", None))

        return flat_schema

    # ....................... #

    @classmethod
    def model_reference(
        cls,
        include: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
        extra: Optional[list[str]] = None,
        extra_definitions: list[dict[str, str]] = [],
        prefix: str = "",
    ):
        """
        Generate a reference schema for the model data structure with extra definitions

        Args:
            include (list[str], optional): The fields to include in the schema. Defaults to None.
            exclude (list[str], optional): The fields to exclude from the schema. Defaults to None.
            extra (list[str], optional): The extra fields to include in the schema. Defaults to None.
            extra_definitions (list[dict[str, str]], optional): The extra definitions to include in the schema. Defaults to [].

        Returns:
            schema (BaseReference): The reference schema for the model
        """

        schema = cls.model_flat_schema(include, exclude, extra, extra_definitions)

        if prefix:
            schema = [
                {**s, "key": f"{prefix}_{s['key']}"}
                for s in schema
                if s["key"] not in (extra or [])
            ]

        return BaseReference(table_schema=schema)

    # ....................... #

    @staticmethod
    def _handle_secret(x: Any) -> Any:
        """
        Handle secret values recursively

        Args:
            x (Any): The value to handle

        Returns:
            Any: The handled value
        """

        if isinstance(x, SecretStr):
            return x.get_secret_value()

        elif isinstance(x, dict):
            return {k: Base._handle_secret(v) for k, v in x.items()}

        elif isinstance(x, (list, set, tuple)):
            return [Base._handle_secret(v) for v in x]

        else:
            return x

    # ....................... #

    def model_dump_with_secrets(self: Self):
        """
        Dump the model with secrets

        Returns:
            data (dict[str, Any]): The model data with secrets
        """

        res = self.model_dump()

        for k, v in res.items():
            res[k] = self._handle_secret(v)

        return res

    # ....................... #

    @classmethod
    def model_validate_universal(
        cls,
        data: dict[str, Any] | str | Self,
    ):
        """
        Validate the model data in a universal way

        Args:
            data (dict[str, Any] | str | Base): The data to validate

        Returns:
            model (Base): The validated
        """

        if isinstance(data, str):
            return cls.model_validate_json(data)

        elif isinstance(data, dict):
            return cls.model_validate(data)

        else:
            return cls.model_validate(data, from_attributes=True)

    # ....................... #

    @classmethod
    def _define_dtype(
        cls,
        key: str,
        dtype: Optional[str] = None,
    ):
        """
        Define the data type of a given key

        Args:
            key (str): The key to define the type for
            dtype (str, optional): The dtype corresponding to the key. Defaults to None.

        Returns:
            type (str): The data type of the given key
        """

        for k, v in cls.specific_fields.items():
            if key in v:
                return k

        if dtype is not None:
            return dtype

        else:
            return "string"


# ....................... #


class BaseReference(BaseModel):
    table_schema: list[dict[str, str]] = []

    # ....................... #

    def merge(self: Self, *others: Self):
        """
        Merge two references

        Args:
            others (BaseReference): The other references

        Returns:
            schema (BaseReference): The merged reference
        """

        for sch in others:
            keys = [f["key"] for f in self.table_schema]
            update = [x for x in sch.table_schema if x["key"] not in keys]
            self.table_schema.extend(update)

        return self

    # ....................... #

    @model_validator(mode="before")
    @classmethod
    def filter_schema_fields(cls, v: dict[str, Any]):
        v["table_schema"] = [
            {k: v for k, v in field.items() if k in ["key", "title", "type"]}
            for field in v["table_schema"]
        ]

        return v


# ....................... #


@json_schema_modifier(trim_description)
class TableResponse(BaseModel):
    """
    Response for a table query

    Attributes:
        hits (TabularData): The hits of the query
        size (int): The page size of the query
        page (int): The page number of the query
        count (int): The total count of the query result
    """

    hits: TabularData = Field(default_factory=TabularData)
    size: int
    page: int
    count: int

    # ....................... #

    @classmethod
    def example(cls, hit: dict[str, Any]):
        return {
            "hits": [hit] * 2,
            "size": 2,
            "page": 1,
            "count": 100,
        }
