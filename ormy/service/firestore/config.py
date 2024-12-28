from typing import Optional

import firebase_admin  # type: ignore
from pydantic import ConfigDict

from ormy.base.abc import ConfigABC
from ormy.base.pydantic import Base

# ----------------------- #


class FirestoreCredentials(Base):
    """
    Firestore connect credentials

    Attributes:
        project_id (str): Firebase project ID
        app (firebase_admin.App, optional): Firebase app to bind
        app_name (str, optional): Firebase app name
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
            self.project_id = self.app.project_id  # type: ignore

        return self


# ....................... #


class FirestoreConfig(ConfigABC):
    """
    Configuration for Firestore Base Model

    Attributes:
        database (str): Database name to assign
        collection (str): Collection name to assign
        include_to_registry (bool): Whether to include to registry
        credentials (ormy.service.firestore.FirestoreCredentials): Firestore connection credentials
    """

    # Local configuration
    database: str = "(default)"
    collection: str = "default"
    include_to_registry: bool = True

    # Global configuration
    credentials: FirestoreCredentials = FirestoreCredentials()

    # ....................... #

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not self.is_default():
            self.credentials.validate_app()

    # ....................... #

    def is_default(self) -> bool:
        """
        Validate if the config is default
        """

        return self._default_helper("collection")
