import logging
from typing import Any, Dict, List, Optional, Type, TypeVar

from infi.clickhouse_orm import migrations  # type: ignore[import-untyped]

from .wrapper import ClickHouseBase, ClickHouseModel

# ----------------------- #

ChB = TypeVar("ChB", bound=ClickHouseBase)
ChM = TypeVar("ChM", bound=ClickHouseModel)

logger = logging.getLogger("migrations")  # TODO: refactor

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


# ....................... #


class ModelOperation(migrations.ModelOperation):
    def __init__(self, model_class: Type[ChM] | Type[ChB]):
        if issubclass(model_class, ClickHouseBase):
            model_class = model_class._model  # type: ignore

        super().__init__(model_class)


# ....................... #


class CreateTable(ModelOperation, migrations.CreateTable):
    pass


# ....................... #
