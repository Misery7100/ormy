# from typing import Optional

# from camel_converter.pydantic_base import CamelBase
# from pydantic import EmailStr

# # ----------------------- #


# class FirebaseUserInfo(CamelBase):
#     """
#     Firebase User Info

#     Attributes:
#         kind (str): The kind of the user.
#         local_id (str): The local ID of the user.
#         email (str): The email of the user.
#         display_name (str, optional): The display name of the user.
#         id_token (str): The ID token of the user.
#         registered (bool, optional): The registration status of the user.
#         refresh_token (str): The refresh token of the user.
#         expires_in (str): The expiration time of the user.
#         expires_at (int, optional): The expiration timestamp of the user.
#     """

#     kind: str
#     local_id: str
#     email: EmailStr
#     display_name: Optional[str] = None
#     id_token: str
#     registered: Optional[bool] = None
#     refresh_token: str
#     expires_in: str
#     expires_at: Optional[int] = None


# # ....................... #


# class FirebaseAccessCredentials(CamelBase):
#     """
#     Firebase Access Credentials

#     Attributes:
#         sub (str): The subject of the credentials.
#         email (str): The email of the credentials.
#         name (str): The name of the credentials.
#         email_verified (bool): The email verification status of the credentials.
#         exp (int): The expiration time of the credentials.
#         iat (int): The issued at time of the credentials.
#         auth_time (int): The authentication time of the credentials.
#         uid (str): The user ID of the credentials.
#     """

#     sub: str
#     email: EmailStr
#     name: str
#     email_verified: bool
#     exp: int
#     iat: int
#     auth_time: int
#     uid: str


# # ....................... #


# class FirebaseRefreshCredentials(CamelBase):
#     """
#     Firebase Refresh Credentials

#     Attributes:
#         local_id (str): The local ID of the credentials.
#         id_token (str): The ID token of the credentials.
#         refresh_token (str): The refresh token of the credentials.
#         expires_in (str): The expiration time of the credentials.
#         expires_at (int): The expiration timestamp of the credentials.
#     """

#     local_id: str
#     id_token: str
#     refresh_token: str
#     expires_in: str
#     expires_at: int
