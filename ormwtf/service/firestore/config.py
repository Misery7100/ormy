from typing import Optional

from google.auth.credentials import Credentials
from pydantic import ConfigDict

from ormwtf.base.pydantic import Base

# ----------------------- #


class FirestoreCredentials(Base):
    """
    Firestore connect credentials

    Attributes:
        project_id (str): Firestore project ID
        credentials (Credentials): Firestore credentials instance
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # ....................... #

    project_id: Optional[str] = None
    credentials: Optional[Credentials] = None


# ....................... #


class FirestoreConfig(Base):
    """
    Firestore Configuration for ORM WTF Base Model

    Attributes:
        database (str): Database name to assign
        collection (str): Collection name to assign
        credentials (FirestoreCredentials): Connection credentials
    """

    # Local configuration
    database: Optional[str] = None
    collection: str = "default"

    # Global configuration
    credentials: FirestoreCredentials = FirestoreCredentials()
