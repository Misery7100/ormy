from typing import (  # noqa: F401
    Annotated,
    Any,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    Type,
    TypeVar,
)

from pydantic import BaseModel, ConfigDict

from ormwtf.base.typing import FieldDataType, FieldName, Wildcard

# ----------------------- #

T = TypeVar("T", bound="Base")

# ....................... #


class Base(BaseModel):
    """
    Base class for all Pydantic models within the package

    TODO: write about the `specific_fields` attribute
    """

    model_config = ConfigDict(
        use_enum_values=True,
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
        include: List[FieldName | Wildcard] = ["*"],
        exclude: List[FieldName] = [],
    ):
        """
        Generate a simple schema for flat data models including
        field name (`key`), field title (`title`) and field data type (`type`)
        """

        schema = cls.model_json_schema()

        if not exclude and include == ["*"]:
            schema = [
                {
                    "key": k,
                    "title": v["title"],
                    "type": cls.define_type(k, v.get("type", None)),
                }
                for k, v in schema["properties"].items()
            ]

        elif not exclude:
            schema = [
                {
                    "key": k,
                    "title": v["title"],
                    "type": cls.define_type(k, v.get("type", None)),
                }
                for k, v in schema["properties"].items()
                if k in include
            ]

        else:
            schema = [
                {
                    "key": k,
                    "title": v["title"],
                    "type": cls.define_type(k, v.get("type", None)),
                }
                for k, v in schema["properties"].items()
                if k not in exclude
            ]

        return schema

    # ....................... #

    @classmethod
    def _define_type(
        cls: Type[T],
        key: FieldName,
        pydantic_dtype: Optional[FieldDataType] = None,
    ) -> FieldDataType:
        """Determine field data type"""

        for k, v in cls.specific_fields.items():
            if key in v:
                return k

        if pydantic_dtype is not None:
            return pydantic_dtype

        else:
            return "string"

    # ....................... #
