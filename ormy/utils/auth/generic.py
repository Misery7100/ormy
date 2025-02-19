# from pydantic import BaseModel, EmailStr, field_validator

# # ----------------------- #


# class SignInWithEmailAndPassword(BaseModel):
#     email: EmailStr
#     password: str


# # ....................... #


# class SignUpWithEmailAndPassword(BaseModel):
#     email: EmailStr
#     username: str
#     password: str

#     # ....................... #

#     @field_validator("username")
#     @classmethod
#     def validate_user_name(cls, v: str):
#         valid_grammar = set("abcdefghijklmnopqrstuvwxyz0123456789_-")

#         if not (len(v) >= 4 or len(v) <= 25):
#             raise ValueError("User name must be from 4 to 25 symbols long")

#         if str(v[-1]) not in ["_", "-"]:
#             raise ValueError("User name cannot end with `_`")

#         if not all(ch.lower() in valid_grammar for ch in v):
#             raise ValueError("User name can only contain letters, numbers, `_` and `-`")

#         return v

#     # ....................... #

#     @field_validator("password")
#     @classmethod
#     def validate_password(cls, v: str):
#         if len(v) < 8:
#             raise ValueError("Password must be at least 8 characters long")

#         return v
