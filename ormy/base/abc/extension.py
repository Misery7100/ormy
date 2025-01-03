import inspect
import logging
from abc import ABC
from typing import Any, ClassVar, Dict, List, Type, TypeVar

from ormy.base.error import InternalError
from ormy.base.pydantic import Base
from ormy.utils.logging import LogLevel, LogManager

from .config import ConfigABC
from .func import merge_registry_helper, register_subclass

# ----------------------- #

E = TypeVar("E", bound="ExtensionABC")
C = TypeVar("C", bound=ConfigABC)

# ----------------------- #


class ExtensionABC(Base, ABC):
    """
    Extension ABC Base Class
    """

    extension_configs: ClassVar[List[Any]] = []
    include_to_registry: ClassVar[bool] = True
    _registry: ClassVar[Dict[Any, dict]] = {}
    _logger: ClassVar[logging.Logger] = LogManager.get_logger("ExtensionABC")

    # ....................... #

    def __init_subclass__(cls: Type[E], **kwargs):
        """Initialize subclass"""

        cls._logger = LogManager.get_logger(cls.__name__)

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
    def set_log_level(cls: Type[E], level: LogLevel) -> None:
        """
        Set the log level for the logger

        Args:
            level (ormy.utils.logging.LogLevel): The new log level
        """

        LogManager.update_log_level(cls.__name__, level)

    # ....................... #

    @classmethod
    def get_extension_config(cls: Type[E], type_: Type[C]) -> C:
        """
        Get configuration for the given type

        Args:
            type_ (Type[ConfigABC]): Type of the configuration

        Returns:
            config (ConfigABC): Configuration
        """

        cfg = next((c for c in cls.extension_configs if type(c) is type_), None)

        if cfg is None:
            msg = f"Configuration {type_} for {cls.__name__} not found"
            cls._logger.error(msg)

            raise InternalError(msg)

        cls._logger.debug(f"Configuration for {cls.__name__}: {type(cfg)}")

        return cfg

    # ....................... #

    @classmethod
    def _update_ignored_types_extension(cls: Type[E]):
        """Update ignored types for the model configuration"""

        ignored_types = cls.model_config.get("ignored_types", tuple())

        for x in cls.extension_configs:
            if (tx := type(x)) not in ignored_types:
                ignored_types += (tx,)

        cls.model_config["ignored_types"] = ignored_types

        cls._logger.debug(f"Ignored types for {cls.__name__}: {ignored_types}")

    # ....................... #

    @classmethod
    def _merge_extension_configs(cls: Type[E]):
        """Merge configurations for the subclass"""

        parents = inspect.getmro(cls)[1:]
        cfgs = []

        for p in parents:
            if hasattr(p, "_registry") and hasattr(p, "extension_configs"):
                cfgs = p.extension_configs
                break

        cls._logger.debug(f"Parent configs for {cls.__name__}: {list(map(type, cfgs))}")

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

            cls._logger.debug(f"Self: {c}")
            cls._logger.debug(f"Parent: {old}")
            cls._logger.debug(f"Merge: {merge}")

        cls.extension_configs = merged

    # ....................... #

    @classmethod
    def _merge_registry_helper(cls: Type[E], d1: dict, d2: dict) -> dict:
        """Merge registry for the subclass"""

        return merge_registry_helper(
            cls=cls,
            d1=d1,
            d2=d2,
            logger=cls._logger,
        )

    # ....................... #

    @classmethod
    def _merge_registry(cls: Type[E]):
        """Merge registry for the subclass"""

        parents = inspect.getmro(cls)[1:]
        reg = dict()

        for p in parents:
            if hasattr(p, "_registry"):
                reg = p._registry

        cls._logger.debug(f"Parent registry for {cls.__name__}: {reg}")
        cls._logger.debug(f"Self registry for {cls.__name__}: {cls._registry}")

        cls._registry = merge_registry_helper(
            cls=cls,
            d1=reg,
            d2=cls._registry,
            logger=cls._logger,
        )

    # ....................... #

    @classmethod
    def _register_extension_subclass_helper(
        cls: Type[E],
        config: Type[C],
        discriminator: str | List[str],
    ):
        """
        Register subclass in the registry

        Args:
            config (Type[C]): Configuration
            discriminator (str): Discriminator
        """

        cfg = cls.get_extension_config(type_=config)

        register_subclass(
            cls=cls,
            config=cfg,
            discriminator=discriminator,
            logger=cls._logger,
        )
