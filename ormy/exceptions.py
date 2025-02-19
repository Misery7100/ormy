from typing import Optional

# ----------------------- #


class OrmyError(Exception):
    """Base class for all exceptions raised by the package"""

    def __init__(self, detail: str, status_code: Optional[int] = None) -> None:
        self.detail = detail
        self.status_code = status_code

    # ....................... #

    def __str__(self) -> str:
        return f"{self.status_code or 'Error'}: {self.detail}"


# ....................... #


class NotFound(OrmyError):
    """Exception raised when a resource is not found."""

    def __init__(self, detail: str = "Resource not found") -> None:
        super().__init__(detail, status_code=404)


# ....................... #


class InternalError(OrmyError):
    """Exception raised for an internal server error."""

    def __init__(self, detail: str = "An internal error occurred"):
        super().__init__(detail, status_code=500)


# ....................... #


class BadRequest(OrmyError):
    """Exception raised for invalid input."""

    def __init__(self, detail: str = "Invalid input"):
        super().__init__(detail, status_code=400)


# ....................... #


class Unauthorized(OrmyError):
    """Exception raised for unauthorized access."""

    def __init__(self, detail: str = "Unauthorized access"):
        super().__init__(detail, status_code=401)


# ....................... #


class Forbidden(OrmyError):
    """Exception raised for forbidden access."""

    def __init__(self, detail: str = "Access forbidden"):
        super().__init__(detail, status_code=403)


# ....................... #


class Conflict(OrmyError):
    """Exception raised for a conflict (e.g., duplicate entry)."""

    def __init__(self, detail: str = "Conflict occurred"):
        super().__init__(detail, status_code=409)


# ....................... #


class ModuleNotFound(ModuleNotFoundError):
    """Exception raised when a module is not found."""

    def __init__(self, extra: str, packages: list[str]):
        name = "Package" if len(packages) == 1 else "Packages"
        art = "is" if len(packages) == 1 else "are"
        p = ", ".join(f"`{p}`" for p in packages)

        super().__init__(
            f"{name} {p} {art} part of the `{extra}` extra. Install it using `pip install ormy[{extra}]`."
        )
