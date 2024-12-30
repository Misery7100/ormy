import inspect
from abc import ABC, abstractmethod  # noqa: F401
from typing import Any, ClassVar, Dict, List, Optional, Type, TypeVar

from ormy.base.error import InternalError
from ormy.base.pydantic import Base
from ormy.utils.logging import LogLevel, console_logger

from .config import ConfigABC
from .func import merge_registry_helper

# ----------------------- #

A = TypeVar("A", bound="AbstractABC")
As = TypeVar("As", bound="AbstractSingleABC")
C = TypeVar("C", bound=ConfigABC)

logger = console_logger(__name__, level=LogLevel.INFO)

# ----------------------- #


class AbstractABC(Base, ABC):
    """Abstract ABC Base Class"""

    configs: ClassVar[List[Any]] = list()
    include_to_registry: ClassVar[bool] = True
    _registry: ClassVar[Dict[Any, dict]] = {}

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
            logger.error(msg)

            raise InternalError(msg)

        logger.debug(f"Configuration for {cls.__name__}: {type(cfg)}")

        return cfg

    # ....................... #

    def __init_subclass__(cls: Type[A], **kwargs):
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

        logger.debug(f"Ignored types for {cls.__name__}: {ignored_types}")

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

        logger.debug(f"Parent configs for {cls.__name__}: {list(map(type, cfgs))}")

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

            logger.debug(f"Self: {c}")
            logger.debug(f"Parent: {old}")
            logger.debug(f"Merge: {merge}")

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
                    logger.debug(f"Overwriting {k} in registry: {d1[k]} -> {d2[k]}")

            else:
                d1[k] = d2[k]
                logger.debug(f"Adding {k} in registry: {d1[k]} -> {d2[k]}")

        return d1

    # ....................... #

    @classmethod
    def _merge_registry(cls: Type[A]):
        parents = inspect.getmro(cls)[1:]
        reg = dict()

        for p in parents:
            if hasattr(p, "_registry"):
                reg = p._registry

        logger.debug(f"Parent registry for {cls.__name__}: {reg}")
        logger.debug(f"Self registry for {cls.__name__}: {cls._registry}")

        cls._registry = cls._merge_registry_helper(reg, cls._registry)


# ----------------------- #
# ----------------------- #
# ----------------------- #


class AbstractSingleABC(Base, ABC):
    """Abstract ABC Base Class"""

    config: ClassVar[Optional[Any]] = None
    include_to_registry: ClassVar[bool] = True

    _registry: ClassVar[Dict[Any, dict]] = {}

    # ....................... #

    def __init_subclass__(cls: Type[As], **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)

        cls._update_ignored_types()
        cls._merge_configs()

    # ....................... #

    @classmethod
    def _update_ignored_types(cls: Type[As]):
        """Update ignored types for the model configuration"""

        ignored_types = cls.model_config.get("ignored_types", tuple())

        if (tx := type(cls.config)) not in ignored_types:
            ignored_types += (tx,)

        cls.model_config["ignored_types"] = ignored_types

        logger.debug(f"Ignored types for {cls.__name__}: {ignored_types}")

    # ....................... #

    @classmethod
    def _merge_configs(cls: Type[As]):
        """Merge configurations for the subclass"""

        parents = inspect.getmro(cls)[1:]
        parent_config = None

        for p in parents:
            if hasattr(p, "_registry") and hasattr(p, "config"):
                parent_config = p.config
                break

        if parent_config is None:
            logger.debug(f"Parent config for {cls.__name__} not found")
            return

        if cls.config is not None:
            merged_config = cls.config.merge(parent_config)
            logger.debug(
                f"Merging configs for {cls.__name__}: {cls.config} <- {parent_config}"
            )

        else:
            merged_config = parent_config
            logger.debug(f"Using parent config for {cls.__name__}: {parent_config}")

        cls.config = merged_config
        logger.debug(f"Final config for {cls.__name__}: {merged_config}")

    # ....................... #

    @classmethod
    def _merge_registry_helper(cls: Type[As], d1: dict, d2: dict) -> dict:
        """Merge registry for the subclass"""

        return merge_registry_helper(
            cls=cls,
            d1=d1,
            d2=d2,
            logger=logger,
        )

    # ....................... #

    @classmethod
    def _merge_registry(cls: Type[As]):
        """
        Merge registry for the subclass
        """

        parents = inspect.getmro(cls)[1:]
        reg = dict()

        for p in parents:
            if hasattr(p, "_registry"):
                reg = p._registry
                break

        logger.debug(f"Parent registry for {cls.__name__}: {reg}")
        logger.debug(f"Self registry for {cls.__name__}: {cls._registry}")

        cls._registry = merge_registry_helper(
            cls=cls,
            d1=reg,
            d2=cls._registry,
            logger=logger,
        )
