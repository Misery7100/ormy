from ormy.exceptions import InternalError

# ----------------------- #


class BigQueryInsertError(InternalError):
    """BigQuery insert error"""


# ....................... #


class BigQueryBackendInsertError(BigQueryInsertError):
    """BigQuery backend insert error"""


# ....................... #


class BigQueryFetchError(InternalError):
    """BigQuery fetch error"""
