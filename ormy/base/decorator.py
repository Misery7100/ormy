from typing import Callable

from pydantic import GetJsonSchemaHandler
from pydantic_core import CoreSchema

# ----------------------- #


def json_schema_modifier(*modifiers: Callable[[dict], dict]):
    def decorator(cls):
        original_get_schema = getattr(cls, "__get_pydantic_json_schema__", None)

        def _custom_schema(cls_, schema: CoreSchema, handler: GetJsonSchemaHandler):
            json_schema = handler(schema)
            json_schema = handler.resolve_ref_schema(json_schema)

            if original_get_schema:
                json_schema = original_get_schema(schema, handler)

            for modifier in modifiers:
                json_schema = modifier(json_schema)

            return json_schema

        cls.__get_pydantic_json_schema__ = classmethod(_custom_schema)

        return cls

    return decorator


# ....................... #


def remove_description(schema: dict) -> dict:
    schema.pop("description", None)

    return schema


# ....................... #


def trim_description(schema: dict) -> dict:
    if "description" in schema:
        schema["description"] = schema["description"].split("\n\n")[0]

    return schema
