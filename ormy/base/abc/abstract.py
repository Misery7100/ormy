import inspect
import logging
from abc import ABC, abstractmethod  # noqa: F401
from typing import Any, ClassVar, Dict, List, Optional, Type, TypeVar

from ormy.base.error import InternalError
from ormy.base.logging import LogLevel, LogManager
from ormy.base.pydantic import Base

from .config import ConfigABC
from .registry import Registry

# ----------------------- #

A = TypeVar("A", bound="AbstractABC")
As = TypeVar("As", bound="AbstractSingleABC")
C = TypeVar("C", bound=ConfigABC)

# ----------------------- #


class AbstractABC(Base, ABC):
    """Abstract ABC Base Class"""

    configs: ClassVar[List[Any]] = list()
    include_to_registry: ClassVar[bool] = True
    _registry: ClassVar[Dict[Any, dict]] = {}
    _logger: ClassVar[logging.Logger] = LogManager.get_logger("AbstractABC")

    # ....................... #

    @classmethod
    def set_log_level(cls: Type[A], level: LogLevel) -> None:
        """
        Set the log level for the logger

        Args:
            level (ormy.utils.logging.LogLevel): The new log level
        """

        LogManager.update_log_level(cls.__name__, level)

    # ....................... #

    @classmethod
    def get_config(cls: Type[A], type_: Type[C]) -> C:
        """
        Get configuration for the given type

        Args:
            type_ (Type[C]): Type of the configuration

        Returns:
            config (C): Configuration
        """

        cfg = next((c for c in cls.configs if type(c) is type_), None)

        if cfg is None:
            msg = f"Configuration {type_} for {cls.__name__} not found"
            cls._logger.error(msg)

            raise InternalError(msg)

        return cfg

    # ....................... #

    def __init_subclass__(cls: Type[A], **kwargs):
        cls._logger = LogManager.get_logger(cls.__name__)

        super().__init_subclass__(**kwargs)

        cls._update_ignored_types()
        cls._merge_configs()

    # ....................... #

    @classmethod
    def _update_ignored_types(cls: Type[A]):
        """
        Update ignored types for the model configuration
        """

        ignored_types = cls.model_config.get("ignored_types", tuple())

        for x in cls.configs:
            if (tx := type(x)) not in ignored_types:
                ignored_types += (tx,)

        cls.model_config["ignored_types"] = ignored_types

        cls._logger.debug(f"Ignored types for {cls.__name__}: {ignored_types}")

    # ....................... #

    @classmethod
    def _merge_configs(cls: Type[A]):
        """
        ...
        """

        parents = inspect.getmro(cls)[1:]
        cfgs = []

        for p in parents:
            if hasattr(p, "_registry") and hasattr(p, "configs"):
                cfgs = p.configs
                break

        cls._logger.debug(f"Parent configs for {cls.__name__}: {list(map(type, cfgs))}")

        deduplicated = dict()

        for c in cls.configs:
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

            cls._logger.debug(f"Self: {c}")
            cls._logger.debug(f"Parent: {old}")
            cls._logger.debug(f"Merge: {merge}")

        cls.configs = merged

    # ....................... #

    @classmethod
    def _merge_registry_helper(cls: Type[A], d1: dict, d2: dict) -> dict:
        for k in d2.keys():
            if k in d1:
                if isinstance(d1[k], dict) and isinstance(d2[k], dict):
                    cls._merge_registry_helper(d1[k], d2[k])

                else:
                    d1[k] = d2[k]
                    cls._logger.debug(
                        f"Overwriting {k} in registry: {d1[k]} -> {d2[k]}"
                    )

            else:
                d1[k] = d2[k]
                cls._logger.debug(f"Adding {k} in registry: {d1[k]} -> {d2[k]}")

        return d1

    # ....................... #

    @classmethod
    def _merge_registry(cls: Type[A]):
        parents = inspect.getmro(cls)[1:]
        reg = dict()

        for p in parents:
            if hasattr(p, "_registry"):
                reg = p._registry

        cls._logger.debug(f"Parent registry for {cls.__name__}: {reg}")
        cls._logger.debug(f"Self registry for {cls.__name__}: {cls._registry}")

        cls._registry = cls._merge_registry_helper(reg, cls._registry)


# ----------------------- #
# ----------------------- #
# ----------------------- #


class AbstractSingleABC(Base, ABC):
    """Abstract ABC Base Class"""

    config: ClassVar[Optional[Any]] = None

    _logger: ClassVar[logging.Logger] = LogManager.get_logger("AbstractSingleABC")

    # ....................... #

    def __init_subclass__(cls: Type[As], **kwargs):
        """Initialize subclass"""

        cls._logger = LogManager.get_logger(cls.__name__)

        super().__init_subclass__(**kwargs)

        cls._update_ignored_types()
        cls._merge_configs()

        if cls.config is not None:
            cls.set_log_level(cls.config.log_level)

    # ....................... #

    @classmethod
    def set_log_level(cls: Type[As], level: LogLevel) -> None:
        """
        Set the log level for the logger

        Args:
            level (ormy.utils.logging.LogLevel): The new log level
        """

        LogManager.update_log_level(cls.__name__, level)

    # ....................... #

    @classmethod
    def _update_ignored_types(cls: Type[As]):
        """Update ignored types for the model configuration"""

        ignored_types = cls.model_config.get("ignored_types", tuple())

        if (tx := type(cls.config)) not in ignored_types:
            ignored_types += (tx,)

        cls.model_config["ignored_types"] = ignored_types
        cls._logger.debug(f"Ignored types for {cls.__name__}: {ignored_types}")

    # ....................... #

    @classmethod
    def _merge_configs(cls: Type[As]):
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
            cls._logger.debug(f"Parent config for `{cls.__name__}` not found")
            return

        if cls.config is not None:
            merged_config = cls.config.merge(parent_config)
            cls._logger.debug(
                f"Merge config: `{parent_selected.__name__}` -> `{cls.__name__}`"
            )

        else:
            merged_config = parent_config
            cls._logger.debug(f"Use parent config: `{parent_selected.__name__}`")

        cls.config = merged_config
        cls._logger.debug(f"Final config for `{cls.__name__}`: {merged_config}")

    # ....................... #

    @classmethod
    def _register_subclass_helper(
        cls: Type[As],
        discriminator: str | List[str],
    ):
        """
        Register subclass in the registry

        Args:
            discriminator (str): Discriminator
        """

        Registry.register(
            discriminator=discriminator,
            value=cls,
            config=cls.config,
            logger=cls._logger,
        )
