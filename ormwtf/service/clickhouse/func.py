from typing import Optional

from .database import AsyncDatabase

# ----------------------- #


def get_clickhouse_db(
    db_name: str,
    db_url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    **kwargs,
):
    return AsyncDatabase(
        db_name=db_name,
        verify_ssl_cert=False,  # TODO: check
        username=username,
        password=password,
        db_url=db_url,
        **kwargs,
    )
