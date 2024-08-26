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
                "title": v["title"],
                "type": cls._define_dtype(k, v.get("type", None)),
            }
            for k, v in schema["properties"].items()
            if k in keys
        ]

        return simple_schema

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
    def model_from_any(
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
