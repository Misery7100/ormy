from typing import Optional

from .database import AsyncDatabase

# ----------------------- #


def get_clickhouse_db(
    db_name: str,
    db_url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    verify_ssl_cert: bool = False,
    **kwargs,
):
    return AsyncDatabase(
        db_name=db_name,
        verify_ssl_cert=verify_ssl_cert,  # TODO: check
        username=username,
        password=password,
        db_url=db_url,
        **kwargs,
    )
