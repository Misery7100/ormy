from pydantic import GetJsonSchemaHandler
from pydantic_core import CoreSchema

# ----------------------- #


class NoDocMixin:
    """Mixin to remove the OpenAPI description"""

    @classmethod
    def __get_pydantic_json_schema__(
        cls,
        schema: CoreSchema,
        handler: GetJsonSchemaHandler,
    ):
        json_schema = handler(schema)
        json_schema = handler.resolve_ref_schema(json_schema)
        json_schema.pop("description", None)

        return json_schema


# ....................... #


class TrimDocMixin:
    """Mixin to trim the OpenAPI description"""

    @classmethod
    def __get_pydantic_json_schema__(
        cls,
        schema: CoreSchema,
        handler: GetJsonSchemaHandler,
    ):
        json_schema = handler(schema)
        json_schema = handler.resolve_ref_schema(json_schema)
        description = json_schema.pop("description", None)

        if description:
            description = description.split("\n\n")[0]
            json_schema["description"] = description

        return json_schema
