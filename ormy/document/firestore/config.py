from typing import Optional

from ormy.exceptions import ModuleNotFound

try:
    from firebase_admin import _DEFAULT_APP_NAME, App, get_app  # type: ignore
except ImportError as e:
    raise ModuleNotFound(extra="firestore", packages=["firebase-admin"]) from e

from pydantic import ConfigDict

from ormy._abc import ConfigABC, Mergeable

# ----------------------- #


class FirestoreCredentials(Mergeable):
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
    app: Optional[App] = None
    app_name: Optional[str] = None

    # ....................... #

    def validate_app(self):
        """Validate Firebase app"""

        if self.app is None:
            self.app = get_app(name=self.app_name or _DEFAULT_APP_NAME)

        elif self.project_id is None:
            self.project_id = self.app.project_id


# ....................... #


class FirestoreConfig(ConfigABC):
    """
    Configuration for Firestore Base Model

    Attributes:
        database (str): Database name to assign
        collection (str): Collection name to assign
        credentials (FirestoreCredentials): Firestore connection credentials
    """

    # Local configuration
    database: str = "_default_"
    collection: str = "_default_"

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

        return self._default_helper("database", "collection")
