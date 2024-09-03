from typing import Any, Dict, List, Optional

from infi.clickhouse_orm import migrations  # type: ignore[import-untyped]

# ----------------------- #


class RunSQLWithSettings(migrations.RunSQL):
    """
    A migration operation that executes arbitrary SQL statements.
    """

    def __init__(
        self,
        sql: str | List[str],
        settings: Optional[Dict[str, Any]] = None,
    ):
        """
        Initializer. The given sql argument must be a valid SQL statement or
        list of statements.
        """
        if isinstance(sql, str):
            sql = [sql]

        assert isinstance(sql, list), "'sql' argument must be string or list of strings"

        self._sql = sql
        self.settings = settings

    # ....................... #

    def apply(self, database):
        migrations.logger.info("    Executing raw SQL operations")

        for item in self._sql:
            database.raw(item, settings=self.settings)