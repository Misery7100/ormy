from typing import ClassVar, Dict, List, Optional, Sequence, Type, TypeVar, get_args

from pydantic import BaseModel, ConfigDict

from .typing import FieldDataType, FieldName, FieldSchema, Wildcard

# ----------------------- #

T = TypeVar("T", bound="Base")

# ....................... #


class Base(BaseModel):
    """
    Base class for all Pydantic models within the package

    TODO: write about the `specific_fields` attribute
    """

    model_config = ConfigDict(validate_assignment=True, validate_default=True)

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
        include: Sequence[FieldName] | Wildcard = ["*"],
        exclude: Sequence[FieldName] = [],
    ) -> Sequence[FieldSchema]:
        """
        Generate a simple schema for the model

        Args:
            include (Sequence[FieldName], optional): The fields to include in the schema. Defaults to "*".
            exclude (Sequence[FieldName], optional): The fields to exclude from the schema. Defaults to [].

        Returns:
            schema (Sequence[Field]): The simple schema for the model
        """

        schema = cls.model_json_schema()

        if include in get_args(Wildcard):
            keys = [k for k, _ in schema["properties"].items()]

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
