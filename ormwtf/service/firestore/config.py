from typing import Optional

from google.auth.credentials import Credentials

from ormwtf.base.abc import TypedDictWithDefaults

# ----------------------- #


class FirestoreCredentials(TypedDictWithDefaults):
    """
    Firestore connect credentials

    Attributes:
        project_id (str): Firestore project ID
        credentials (Credentials): Firestore credentials instance
    """

    project_id: Optional[str]
    credentials: Optional[Credentials]

    # ....................... #

    @classmethod
    def with_defaults(
        cls,
        project_id: Optional[str] = None,
        credentials: Optional[Credentials] = None,
    ):
        """
        Returns a new instance of FirestoreCredentials with overridable defaults
        """

        return cls(
            project_id=project_id,
            credentials=credentials,
        )


# ....................... #


class FirestoreConfig(TypedDictWithDefaults):
    """
    Firestore Configuration for ORM WTF Base Model

    Attributes:
        database (str): Database name to assign
        collection (str): Collection name to assign
        credentials (FirestoreCredentials): Connection credentials
    """

    # Local configuration
    database: Optional[str]
    collection: str

    # Global configuration
    credentials: FirestoreCredentials

    # ....................... #

    @classmethod
    def with_defaults(
        cls,
        database: Optional[str] = None,
        collection: str = "default",
        credentials: FirestoreCredentials = FirestoreCredentials.with_defaults(),
    ):
        """
        Returns a new instance of FirestoreConfig with overridable defaults
        """

        return cls(
            database=database,
            collection=collection,
            credentials=credentials,
        )
