from pydantic import Field
from pydantic_settings import BaseSettings
from typing_extensions import TypedDict

# ----------------------- #


class FirestoreConfigDict(TypedDict):
    database: str
    collection: str


# ----------------------- #


class FirestoreSettings(BaseSettings):
    project_id: str = Field(title="Project ID", alias="projectId")
