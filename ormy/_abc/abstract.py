import inspect
from abc import ABC
from typing import (
    Any,
    ClassVar,
    Literal,
    Optional,
    Self,
    Tuple,
    Type,
    TypeVar,
    get_args,
)

from pydantic import BaseModel, model_validator

from ormy.base.logging import LogLevel, LogManager
from ormy.base.pydantic import IGNORE, Base
from ormy.exceptions import InternalError

from .config import ConfigABC
from .registry import Registry

# ----------------------- #

C = TypeVar("C", bound=ConfigABC)

# ----------------------- #


class AbstractABC(Base, ABC):
    """Abstract ABC Base Class"""

    config: ClassVar[Optional[Any]] = None

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)

        cls.__update_ignored_types()
        cls.__merge_configs()

        if cls.config is not None:
            cls._set_log_level(cls.config.log_level)

    # ....................... #

    @classmethod
    def _logger(cls):
        """Logger"""

        return LogManager.get_logger(cls.__name__)  # type: ignore[attr-defined]

    # ....................... #

    @classmethod
    def _set_log_level(cls, level: LogLevel) -> None:
        """
        Set the log level for the logger

        Args:
            level (ormy.utils.logging.LogLevel): The new log level
        """

        LogManager.update_log_level(cls.__name__, level)

    # ....................... #

    @classmethod
    def __update_ignored_types(cls):
        """Update ignored types for the model configuration"""

        ignored_types = cls.model_config.get("ignored_types", tuple())

        if (tx := type(cls.config)) not in ignored_types:
            ignored_types += (tx,)

        cls.model_config["ignored_types"] = ignored_types
        cls._logger().debug(f"Ignored types for {cls.__name__}: {ignored_types}")

    # ....................... #

    @classmethod
    def __merge_configs(cls):
        """Merge configurations for the subclass"""

        parents = inspect.getmro(cls)[1:]
        parent_config = None
        parent_selected = None

        for p in parents:
            if hasattr(p, "config") and issubclass(type(p.config), ConfigABC):
                parent_config = p.config
                parent_selected = p
                break

        if parent_config is None or parent_selected is None:
            cls._logger().debug(f"Parent config for `{cls.__name__}` not found")
            return

        if cls.config is not None:
            merged_config = cls.config.merge(parent_config)
            cls._logger().debug(
                f"Merge config: `{parent_selected.__name__}` -> `{cls.__name__}`"
            )

        else:
            merged_config = parent_config
            cls._logger().debug(f"Use parent config: `{parent_selected.__name__}`")

        cls.config = merged_config
        cls._logger().debug(f"Final config for `{cls.__name__}`: {merged_config}")

    # ....................... #

    @classmethod
    def _register_subclass_helper(cls, discriminator: str | list[str]):
        """
        Register subclass in the registry

        Args:
            discriminator (str): Discriminator
        """

        Registry.register(discriminator=discriminator, value=cls, config=cls.config)


# ----------------------- #

ValueOperator = Literal["==", "!=", "<", "<=", ">", ">=", "array_contains"]
ArrayOperator = Literal["in", "not_in", "array_contains_any"]
ValueType = Optional[str | bool | int | float]
AbstractContext = Tuple[str, ValueOperator | ArrayOperator, ValueType | list[ValueType]]

# ....................... #


class ContextItem(BaseModel):
    """Context item"""

    operator: ValueOperator | ArrayOperator
    field: str
    value: ValueType | list[ValueType]

    # ....................... #

    @model_validator(mode="after")
    def validate_operator(self):
        """Validate operator"""

        if self.operator in get_args(ValueOperator):
            if isinstance(self.value, list):
                raise InternalError("Value operator cannot be used with list")

        elif self.operator in get_args(ArrayOperator):
            if not isinstance(self.value, list):
                raise InternalError("Array operator must be used with list")

        else:
            raise InternalError(f"Invalid operator: {self.operator}")

        return self

    # ....................... #

    def evaluate(self: Self, model: BaseModel):
        """
        Evaluate context item

        Args:
            model (BaseModel): Model to evaluate

        Returns:
            res (bool): Evaluation result
        """

        model_value = getattr(model, self.field, IGNORE)

        if model_value == IGNORE:
            return False

        if self.operator in get_args(ValueOperator):
            return self.__evaluate_value_operator(model_value)

        elif self.operator in get_args(ArrayOperator):
            return self.__evaluate_array_operator(model_value)

        return False

    # ....................... #

    def __evaluate_value_operator(self: Self, model_value: Any) -> bool:
        """
        Evaluate value operator

        Args:
            model_value (Any): Model value

        Returns:
            res (bool): Evaluation result
        """

        if self.operator == "array_contains":
            if not isinstance(model_value, list):
                raise InternalError(
                    f"Operator `{self.operator}` must be used with list"
                )

            return self.value in model_value

        return eval(f"{model_value} {self.operator} {self.value}")

    # ....................... #

    def __evaluate_array_operator(self: Self, model_value: Any) -> bool:
        """
        Evaluate array operator

        Args:
            model_value (Any): Model value

        Returns:
            res (bool): Evaluation result
        """

        if self.operator == "in":
            return self.value in model_value

        elif self.operator == "not_in":
            return self.value not in model_value

        elif self.operator == "array_contains_any":
            return any(self.value in item for item in model_value)

        return False


# ....................... #


class SemiFrozenField(BaseModel):
    """Semi frozen field"""

    context: Optional[list[AbstractContext] | AbstractContext] = None
    mode: Literal["and", "or"] = "and"

    # ....................... #

    def evaluate(self: Self, model: BaseModel) -> bool:
        """
        Evaluate semi frozen field

        Args:
            model (BaseModel): Model to evaluate

        Returns:
            res (bool): Evaluation result
        """

        if self.context:
            if not isinstance(self.context, list):
                context = [self.context]

            else:
                context = self.context

            res = [
                ContextItem(
                    field=field,
                    operator=operator,
                    value=value,
                ).evaluate(model)
                for field, operator, value in context
            ]

            if self.mode == "and":
                return all(res)

            elif self.mode == "or":
                return any(res)

        return True


# ....................... #


class AbstractExtensionABC(Base, ABC):
    """Abstract Extension ABC Base Class"""

    extension_configs: ClassVar[list[Any]] = []

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)

        cls._update_ignored_types_extension()
        cls._merge_extension_configs()

        min_level = LogLevel.CRITICAL

        for x in cls.extension_configs:
            if x is not None:
                if x.log_level.value < min_level.value:
                    min_level = x.log_level

        cls.set_log_level(min_level)

    # ....................... #

    @classmethod
    def _logger(cls):
        """Logger"""

        return LogManager.get_logger(cls.__name__)  # type: ignore[attr-defined]

    # ....................... #

    @classmethod
    def set_log_level(cls, level: LogLevel) -> None:
        """
        Set the log level for the logger

        Args:
            level (ormy.utils.logging.LogLevel): The new log level
        """

        LogManager.update_log_level(cls.__name__, level)

    # ....................... #

    @classmethod
    def get_extension_config(cls, type_: Type[C]) -> C:
        """
        Get configuration for the given type

        Args:
            type_ (Type[ConfigABC]): Type of the configuration

        Returns:
            config (ConfigABC): Configuration
        """

        cfg = next((c for c in cls.extension_configs if type(c) is type_), None)

        if cfg is None:
            raise InternalError(
                f"Configuration `{type_.__name__}` for `{cls.__name__}` not found"
            )

        return cfg

    # ....................... #

    @classmethod
    def _update_ignored_types_extension(cls):
        """Update ignored types for the model configuration"""

        ignored_types = cls.model_config.get("ignored_types", tuple())

        for x in cls.extension_configs:
            if (tx := type(x)) not in ignored_types:
                ignored_types += (tx,)

        cls.model_config["ignored_types"] = ignored_types

        cls._logger().debug(f"Ignored types for {cls.__name__}: {ignored_types}")

    # ....................... #

    @classmethod
    def _merge_extension_configs(cls):
        """Merge configurations for the subclass"""

        parents = inspect.getmro(cls)[1:]
        cfgs = []
        parent_selected = None

        for p in parents:
            if hasattr(p, "extension_configs") and all(
                issubclass(type(x), ConfigABC) for x in p.extension_configs
            ):
                cfgs = p.extension_configs
                parent_selected = p
                break

        cls._logger().debug(
            f"Parent configs from `{parent_selected.__name__ if parent_selected else None}`: {list(map(lambda x: type(x).__name__, cfgs))}"
        )

        deduplicated = dict()

        for c in cls.extension_configs:
            type_ = type(c)

            if type_ not in deduplicated:
                deduplicated[type_] = c

            else:
                deduplicated[type_] = c.merge(deduplicated[type_])

        merged = []

        for c in deduplicated.values():
            old = next((x for x in cfgs if type(x) is type(c)), None)

            if old is not None:
                merge = c.merge(old)
                merged.append(merge)

            else:
                merge = c
                merged.append(c)

        cls.extension_configs = merged

    # ....................... #

    @classmethod
    def _register_extension_subclass_helper(
        cls,
        config: Type[C],
        discriminator: str | list[str],
    ):
        """
        Register subclass in the registry

        Args:
            config (Type[C]): Configuration
            discriminator (str): Discriminator
        """

        cfg = cls.get_extension_config(type_=config)

        Registry.register(discriminator=discriminator, value=cls, config=cfg)
