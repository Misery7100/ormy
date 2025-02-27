from typing import Any, Optional

from ormy.exceptions import ModuleNotFound

try:
    from infi.clickhouse_orm import migrations  # type: ignore[import-untyped]
except ImportError as e:
    raise ModuleNotFound(extra="clickhouse", packages=["infi-clickhouse-orm"]) from e

from .wrapper import ClickHouseBase

# ----------------------- #


class RunSQLWithSettings(migrations.RunSQL):
    """
    A migration operation that executes arbitrary SQL statements.
    """

    def __init__(
        self,
        sql: str | list[str],
        settings: Optional[dict[str, Any]] = None,
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
    def __init__(self, model_class: Any):
        if issubclass(model_class, (ClickHouseBase)):
            model_class = model_class._model  # type: ignore

        super().__init__(model_class)


# ....................... #


class CreateTable(ModelOperation, migrations.CreateTable):
    pass


class AlterTable(ModelOperation, migrations.AlterTable):
    pass


class AlterTableWithBuffer(ModelOperation, migrations.AlterTableWithBuffer):
    pass


class DropTable(ModelOperation, migrations.DropTable):
    pass


class AlterConstraints(ModelOperation, migrations.AlterConstraints):
    pass


class AlterIndexes(ModelOperation, migrations.AlterIndexes):
    pass


class RunPython(ModelOperation, migrations.RunPython):
    pass
