import inspect
from abc import ABC, ABCMeta
from typing import (
    Any,
    Callable,
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
from pydantic._internal import _model_construction

from ormy.base.logging import LogLevel, LogManager
from ormy.base.pydantic import IGNORE, Base
from ormy.exceptions import InternalError

from .config import ConfigABC
from .registry import Registry

# ----------------------- #

C = TypeVar("C", bound=ConfigABC)

# ----------------------- #


class AbstractABCMeta(_model_construction.ModelMetaclass, ABCMeta):
    def __init__(cls, name, bases, namespace, **kwargs):
        super().__init__(name, bases, namespace, **kwargs)  # type: ignore[call-arg]
        AbstractABCMeta._init(cls)

    # ....................... #

    @staticmethod
    def _init(cls: Any):
        # AbstractABC
        if callable(getattr(cls, "_update_ignored_types", None)):
            cls._update_ignored_types()

        if callable(getattr(cls, "_merge_config", None)):
            cls._merge_config()

        # AbstractMixinABC
        if callable(getattr(cls, "_update_ignored_types_mixin", None)):
            cls._update_ignored_types_mixin()

        if callable(getattr(cls, "_merge_mixin_configs", None)):
            cls._merge_mixin_configs()

        # Run subclass registration
        if hasattr(cls, "__discriminator__"):
            if callable(getattr(cls, "_register_subclass", None)):
                if (
                    cls.config is not None
                    and not cls.config.is_default()
                    and cls.__discriminator__
                ):
                    cls._register_subclass(discriminator=cls.__discriminator__)

        # Run deferred mixin config patches
        for config_type, field, compute_fn in getattr(
            cls, "_pending_config_patches", []
        ):
            try:
                cfg = cls.get_mixin_config(type_=config_type)

            except InternalError:
                cfg = config_type()

            other_ext_configs = [x for x in cls.mixin_configs if x not in [cfg]]

            if cls.config is not None and not cls.config.is_default():
                assert hasattr(
                    cfg, field
                ), f"Field `{field}` not found in `{config_type.__name__}`"

                setattr(cfg, field, compute_fn(cls))

            cls.mixin_configs = [cfg] + other_ext_configs

        cls._pending_config_patches = []

        # Run deferred mixin registrations
        for fn, args in getattr(cls, "_pending_mixin_registrations", []):
            try:
                fn(**args)
            except InternalError as e:
                cls._logger().warning(
                    f"Skipped registration for `{args['config'].__name__}` in `{cls.__name__}`: {e}"
                )

        cls._pending_mixin_registrations = []

        # Logging
        if getattr(cls, "config", None) is not None:
            if callable(getattr(cls, "_set_log_level", None)):
                cls._set_log_level(cls.config.log_level)

        elif hasattr(cls, "mixin_configs"):  #! Actually redundant more or less
            min_level = LogLevel.CRITICAL

            for x in cls.mixin_configs:
                if x is not None and x.log_level.value < min_level.value:
                    min_level = x.log_level

            if callable(getattr(cls, "_set_log_level", None)):
                cls._set_log_level(min_level)


# ....................... #


class AbstractABC(Base, ABC, metaclass=AbstractABCMeta):
    """Abstract ABC Base Class"""

    config: ClassVar[Optional[Any]] = None
    __discriminator__: ClassVar[list[str]] = []

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
    def _update_ignored_types(cls):
        """Update ignored types for the model configuration"""

        ignored_types = cls.model_config.get("ignored_types", tuple())

        if (tx := type(cls.config)) not in ignored_types:
            ignored_types += (tx,)

        cls.model_config["ignored_types"] = ignored_types
        cls._logger().debug(f"Ignored types for {cls.__name__}: {ignored_types}")

    # ....................... #

    @classmethod
    def _merge_config(cls):
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
    def _register_subclass(cls, discriminator: str | list[str]):
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
            return self._evaluate_value_operator(model_value)

        elif self.operator in get_args(ArrayOperator):
            return self._evaluate_array_operator(model_value)

        return False

    # ....................... #

    def _evaluate_value_operator(self: Self, model_value: Any) -> bool:
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

    def _evaluate_array_operator(self: Self, model_value: Any) -> bool:
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


class AbstractMixinABC(Base, ABC):
    """Abstract Mixin ABC Base Class"""

    mixin_configs: ClassVar[list[Any]] = []

    _pending_mixin_registrations: ClassVar[list[Any]] = []
    _pending_config_patches: ClassVar[list[Any]] = []

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
    def get_mixin_config(cls, type_: Type[C]) -> C:
        """
        Get configuration for the given type

        Args:
            type_ (Type[ConfigABC]): Type of the configuration

        Returns:
            config (ConfigABC): Configuration
        """

        cfg = next((c for c in cls.mixin_configs if type(c) is type_), None)

        if cfg is None:
            raise InternalError(
                f"Configuration `{type_.__name__}` for `{cls.__name__}` not found"
            )

        return cfg

    # ....................... #

    @classmethod
    def defer_mixin_registration(cls, config: Type[C], discriminator: str | list[str]):
        cls._pending_mixin_registrations.append(
            (
                cls._register_mixin_subclass,
                {"config": config, "discriminator": discriminator},
            )
        )

    # ....................... #

    @classmethod
    def defer_config_patch(
        cls,
        config_type: Type[C],
        dynamic_field: str,
        compute_fn: Callable,
    ):
        cls._pending_config_patches.append(
            (
                config_type,
                dynamic_field,
                compute_fn,
            )
        )

    # ....................... #

    @classmethod
    def _update_ignored_types_mixin(cls):
        """Update ignored types for the model configuration"""

        ignored_types = cls.model_config.get("ignored_types", tuple())

        for x in cls.mixin_configs:
            if (tx := type(x)) not in ignored_types:
                ignored_types += (tx,)

        cls.model_config["ignored_types"] = ignored_types

        cls._logger().debug(f"Ignored types for {cls.__name__}: {ignored_types}")

    # ....................... #

    @classmethod
    def _merge_mixin_configs(cls):
        """Merge configurations for the subclass"""

        parents = inspect.getmro(cls)[1:]
        cfgs = []
        parent_selected = None

        for p in parents:
            if hasattr(p, "mixin_configs") and all(
                issubclass(type(x), ConfigABC) for x in p.mixin_configs
            ):
                cfgs = p.mixin_configs
                parent_selected = p
                break

        cls._logger().debug(
            f"Parent configs from `{parent_selected.__name__ if parent_selected else None}`: {list(map(lambda x: type(x).__name__, cfgs))}"
        )

        deduplicated = dict()

        for c in cfgs + cls.mixin_configs:
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

        cls.mixin_configs = merged

    # ....................... #

    @classmethod
    def _register_mixin_subclass(
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

        cfg = cls.get_mixin_config(type_=config)

        Registry.register(discriminator=discriminator, value=cls, config=cfg)
