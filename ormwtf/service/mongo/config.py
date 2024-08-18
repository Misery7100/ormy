from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings
from typing_extensions import TypedDict

# ----------------------- #


class MongoConfigDict(TypedDict):
    database: str
    collection: str
    streaming: bool


# ----------------------- #


class MongoSettings(BaseSettings):
    host: Optional[str] = Field("localhost", alias="mongo_host")
    port: Optional[int] = Field(None, alias="mongo_external_port")
    username: str = Field(alias="mongo_initdb_root_username")
    password: str = Field(alias="mongo_initdb_root_password")
    replicaset: str = Field("rs0")
