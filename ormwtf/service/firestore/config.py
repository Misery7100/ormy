from typing import Any, Optional

import firebase_admin
from pydantic import ConfigDict, model_validator

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

    @model_validator(mode="before")
    @classmethod
    def validate_app(cls, v: Any):
        """Validate Firebase app"""
        app: Optional[firebase_admin.App] = v.get("app", None)
        app_name = v.get("app_name", None)
        pid = v.get("project_id", None)

        if app is None:
            app: firebase_admin.App = firebase_admin.get_app(
                name=app_name or firebase_admin._DEFAULT_APP_NAME
            )

        if pid is None:
            pid = app.project_id

        v["project_id"] = pid
        v["app"] = app
        v["app_name"] = app.name

        return v


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
    database: Optional[str] = None
    collection: str = "default"

    # Global configuration
    credentials: FirestoreCredentials = FirestoreCredentials()
