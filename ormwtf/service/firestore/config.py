from typing import Optional

from google.auth.credentials import Credentials
from typing_extensions import TypedDict

# ----------------------- #


class FirestoreCredentials(TypedDict):
    project_id: Optional[str]
    credentials: Optional[Credentials]

    # ....................... #

    @classmethod
    def with_defaults(
        cls,
        project_id: Optional[str] = None,
        credentials: Optional[Credentials] = None,
    ):
        return cls(
            project_id=project_id,
            credentials=credentials,
        )


# ....................... #


class FirestoreConfig(TypedDict):

    # Local configuration
    database: Optional[str]
    collection: str

    # Global configuration
    credentials: FirestoreCredentials = FirestoreCredentials.with_defaults()

    # ....................... #

    @classmethod
    def with_defaults(
        cls,
        database: Optional[str] = None,
        collection: str = "default",
        credentials: FirestoreCredentials = FirestoreCredentials.with_defaults(),
    ):
        return cls(
            database=database,
            collection=collection,
            credentials=credentials,
        )
