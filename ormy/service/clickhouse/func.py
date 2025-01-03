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
    """
    Get ClickHouse database

    Args:
        db_name (str): ClickHouse database name
        db_url (str): ClickHouse database URL
        username (str, optional): ClickHouse username
        password (str, optional): ClickHouse password
        verify_ssl_cert (bool): Whether to verify SSL certificate
        **kwargs: Additional keyword arguments

    Returns:
        database (ormy.service.clickhouse.database.AsyncDatabase): ClickHouse database
    """

    return AsyncDatabase(
        db_name=db_name,
        verify_ssl_cert=verify_ssl_cert,  # TODO: check
        username=username,
        password=password,
        db_url=db_url,
        **kwargs,
    )
