from typing import Any, ClassVar, List, Optional, Type, TypeVar  # noqa: F401

from pydantic import BaseModel, ConfigDict  # noqa: F401

# ----------------------- #

T = TypeVar("T", bound="Base")

# ....................... #


class Base(BaseModel):
    model_config = ConfigDict(
        use_enum_values=True,
        validate_assignment=True,
        validate_default=True,
    )

    datetime_fields: ClassVar[List[str]] = [
        "created_at",
        "last_update_at",
        "deadline",
        "timestamp",
    ]

    # ....................... #

    @classmethod
    def model_simple_schema(
        cls: Type[T],
        include: List[str] = ["*"],
        exclude: List[str] = [],
    ):
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
        key: str,
        pydantic_dtype: Optional[Any] = None,
    ) -> str:
        if key in cls.datetime_fields:
            return "datetime"

        elif pydantic_dtype is not None:
            return pydantic_dtype

        else:
            return "string"

    # ....................... #
