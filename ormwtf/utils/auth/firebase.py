from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

# ----------------------- #


class FirebaseUserInfo(BaseModel):
    """
    Firebase User Info

    Attributes:
        kind (str): The kind of the user.
        local_id (str): The local ID of the user.
        email (str): The email of the user.
        display_name (str, optional): The display name of the user.
        id_token (str): The ID token of the user.
        registered (bool, optional): The registration status of the user.
        refresh_token (str): The refresh token of the user.
        expires_in (str): The expiration time of the user.
        expires_at (int, optional): The expiration timestamp of the user.
    """

    model_config = ConfigDict(populate_by_name=True)

    # ....................... #

    kind: str = Field(
        ...,
        title="Kind",
    )
    local_id: str = Field(
        ...,
        validation_alias="localId",
    )
    email: str = Field(
        ...,
        title="Email",
    )
    display_name: Optional[str] = Field(
        default=None,
        alias="displayName",
        title="Display Name",
    )
    id_token: str = Field(
        ...,
        validation_alias="idToken",
        title="ID Token",
    )
    registered: Optional[bool] = Field(
        default=None,
        title="Registered",
    )
    refresh_token: str = Field(
        ...,
        validation_alias="refreshToken",
        title="Refresh Token",
    )
    expires_in: str = Field(
        ...,
        validation_alias="expiresIn",
        title="Expires In",
    )
    expires_at: Optional[int] = Field(
        default=None,
        validation_alias="expiresAt",
        title="Expires At",
    )


# ....................... #


class FirebaseAccessCredentials(BaseModel):
    """
    Firebase Access Credentials

    Attributes:
        sub (str): The subject of the credentials.
        email (str): The email of the credentials.
        name (str): The name of the credentials.
        email_verified (bool): The email verification status of the credentials.
        exp (int): The expiration time of the credentials.
        iat (int): The issued at time of the credentials.
        auth_time (int): The authentication time of the credentials.
        uid (str): The user ID of the credentials.
    """

    model_config = ConfigDict(populate_by_name=True)

    # ....................... #

    sub: str = Field(
        ...,
        title="Subject",
    )
    email: str = Field(
        ...,
        title="Email",
    )
    name: str = Field(
        ...,
        title="Name",
    )
    email_verified: bool = Field(
        ...,
        validation_alias="emailVerified",
        title="Email Verified",
    )
    exp: int = Field(
        ...,
        title="Expiration Time",
    )
    iat: int = Field(
        ...,
        title="Issued At",
    )
    auth_time: int = Field(
        ...,
        validation_alias="authTime",
        title="Authentication Time",
    )
    uid: str = Field(
        ...,
        title="User ID",
    )


# ....................... #


class FirebaseRefreshCredentials(BaseModel):
    """
    Firebase Refresh Credentials

    Attributes:
        local_id (str): The local ID of the credentials.
        id_token (str): The ID token of the credentials.
        refresh_token (str): The refresh token of the credentials.
        expires_in (str): The expiration time of the credentials.
        expires_at (int): The expiration timestamp of the credentials.
    """

    model_config = ConfigDict(populate_by_name=True)

    # ....................... #

    local_id: str = Field(
        ...,
        validation_alias="localId",
        title="Local ID",
    )
    id_token: str = Field(
        ...,
        validation_alias="idToken",
        title="ID Token",
    )
    refresh_token: str = Field(
        ...,
        validation_alias="refreshToken",
        title="Refresh Token",
    )
    expires_in: str = Field(
        ...,
        validation_alias="expiresIn",
        title="Expires In",
    )
    expires_at: int = Field(
        ...,
        validation_alias="expiresAt",
        title="Expires At",
    )
