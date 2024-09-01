from typing import Any, ClassVar, Dict, List, Optional, Type, TypeVar

from pydantic import BaseModel, ConfigDict, SecretStr

from .typing import FieldDataType, FieldName, FieldSchema

# ----------------------- #

T = TypeVar("T", bound="Base")

# ....................... #


class Base(BaseModel):
    """
    Base class for all Pydantic models within the package

    TODO: write about the `specific_fields` attribute
    """

    model_config = ConfigDict(
        validate_assignment=True,
        validate_default=True,
    )

    specific_fields: ClassVar[Dict[FieldDataType, List[FieldName]]] = {
        "datetime": [
            "created_at",
            "last_update_at",
            "deadline",
            "timestamp",
        ]
    }

    # ....................... #

    @classmethod
    def model_simple_schema(
        cls: Type[T],
        include: Optional[List[FieldName]] = None,
        exclude: Optional[List[FieldName]] = None,
    ) -> List[FieldSchema]:
        """
        Generate a simple schema for the model

        Args:
            include (List[FieldName], optional): The fields to include in the schema.
            exclude (List[FieldName], optional): The fields to exclude from the schema.

        Returns:
            schema (List[FieldSchema]): The simple schema for the model
        """

        schema = cls.model_json_schema()

        if include is None:
            keys: List[FieldName] = [k for k, _ in schema["properties"].items()]

        else:
            keys = include

        if exclude:
            keys = [k for k in keys if k not in exclude]

        simple_schema = [
            {
                "key": k,
                "title": v.get("title", k.title()),
                "type": cls._define_dtype(k, v.get("type", None)),
            }
            for k, v in schema["properties"].items()
            if k in keys
        ]

        return simple_schema

    # ....................... #

    @staticmethod
    def parse_json_schema_defs(json_schema: dict):
        defs = json_schema.get("$defs", {})
        extracted_defs: Dict[str, dict] = {}

        for k, v in defs.items():
            if "enum" in v:
                v["value"] = v["enum"]
                v["type"] = "array"  # "enum"
                extracted_defs[k] = v

        return extracted_defs

    # ....................... #

    # @staticmethod
    # def _field_helper():
    #     pass

    # ....................... #

    @classmethod
    def model_flat_schema(
        cls: Type[T],
        include: Optional[List[FieldName]] = None,
        exclude: Optional[List[FieldName]] = None,
    ) -> List[Dict[str, Any]]:
        """
        ...
        """

        schema = cls.model_json_schema()
        defs = cls.parse_json_schema_defs(schema)
        keys: List[FieldName] = [k for k, _ in schema["properties"].items()]
        flat_schema: List[Dict[str, Any]] = []
        schema_keys = ["key", "title", "type", "value"]

        if include is not None:
            keys = include

        elif exclude is not None:
            keys = [k for k in keys if k not in exclude]

        for k, v in schema["properties"].items():
            if k not in keys:
                continue

            type_ = v.get("type", "string")

            if type_ == "array":
                if items := v.get("items", {}):
                    if "$ref" in items.keys():
                        continue

            if refs := v.get("allOf", []):
                if len(refs) > 1:
                    continue

                ref_name = refs[0]["$ref"].split("/")[-1]

                if ref := defs.get(ref_name, {}):
                    data = {"key": k, **ref}
                    data["title"] = v.get("title", data.get("title", k.title()))
                    data = {
                        k: v
                        for k, v in data.items()
                        if k in schema_keys and v is not None
                    }
                    flat_schema.append(data)

            else:
                data = {"key": k, **v}
                data = {
                    k: v for k, v in data.items() if k in schema_keys and v is not None
                }
                flat_schema.append(data)

        # follow up type definition from specific fields
        for field in flat_schema:
            field["type"] = cls._define_dtype(field["key"], field.get("type", None))

        return flat_schema

    # ....................... #

    @staticmethod
    def _handle_secret(x: Any) -> Any:
        if isinstance(x, SecretStr):
            return x.get_secret_value()

        elif isinstance(x, dict):
            return {k: Base._handle_secret(v) for k, v in x.items()}

        elif isinstance(x, (list, set, tuple)):
            return [Base._handle_secret(v) for v in x]

        else:
            return x

    # ....................... #

    def model_dump_with_secrets(self: T) -> Dict[str, Any]:
        """
        Dump the model with secrets

        Returns:
            data (Dict[str, Any]): The model data with secrets
        """

        res = self.model_dump()

        for k, v in res.items():
            res[k] = self._handle_secret(v)

        return res

    # ....................... #

    @classmethod
    def model_validate_universal(
        cls: Type[T],
        data: Dict[str, Any] | str | T,
    ) -> T:
        if isinstance(data, str):
            return cls.model_validate_json(data)

        elif isinstance(data, dict):
            return cls.model_validate(data)

        else:
            return cls.model_validate(data, from_attributes=True)

    # ....................... #

    @classmethod
    def _define_dtype(
        cls: Type[T],
        key: FieldName,
        dtype: Optional[FieldDataType] = None,
    ) -> FieldDataType:
        """
        Define the data type of a given key

        Args:
            key (FieldName): The key to define the type for
            dtype (FieldDataType, optional): The dtype corresponding to the key. Defaults to None.

        Returns:
            type (FieldDataType): The data type of the given key
        """

        for k, v in cls.specific_fields.items():
            if key in v:
                return k

        if dtype is not None:
            return dtype

        else:
            return "string"

    # ....................... #
