from typing import Optional

import firebase_admin  # type: ignore
from pydantic import ConfigDict

from ormwtf.base.pydantic import Base

# ----------------------- #


class FirestoreCredentials(Base):
    """
    Firestore connect credentials

    Attributes:
        project_id (str): Firebase project ID
        credentials (firebase_admin.App): Firebase app to bind
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # ....................... #

    project_id: Optional[str] = None
    app: Optional[firebase_admin.App] = None
    app_name: Optional[str] = None

    # ....................... #

    def validate_app(self):
        """Validate Firebase app"""

        if self.app is None:
            self.app = firebase_admin.get_app(
                name=self.app_name or firebase_admin._DEFAULT_APP_NAME
            )

        if self.project_id is None:
            self.project_id = self.app.project_id


# ....................... #


class FirestoreConfig(Base):
    """
    Firestore Configuration for ORM WTF Base Model

    Attributes:
        database (str): Database name to assign
        collection (str): Collection name to assign
        credentials (ormwtf.service.firestore.FirestoreCredentials): Connection credentials
    """

    # Local configuration
    database: str = "(default)"
    collection: str = "default"
    include_to_registry: bool = True

    # Global configuration
    credentials: FirestoreCredentials = FirestoreCredentials()
