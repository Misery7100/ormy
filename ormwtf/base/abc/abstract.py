import inspect
from abc import ABC, abstractmethod  # noqa: F401
from typing import Any, ClassVar, Dict, List, Type, TypeVar

from ormwtf.base.pydantic import Base
from ormwtf.utils.logging import LogLevel, console_logger

from .config import ConfigABC

# ----------------------- #

A = TypeVar("A", bound="AbstractABC")
C = TypeVar("C", bound=ConfigABC)

logger = console_logger(__name__, level=LogLevel.INFO)


class AbstractABC(Base, ABC):
    """
    Abstract ABC Base Class
    """

    configs: ClassVar[List[Any]] = list()
    include_to_registry: ClassVar[bool] = True

    _config_type: ClassVar[Any] = ConfigABC
    _registry: ClassVar[Dict[Any, dict]] = {}

    # ....................... #

    @classmethod
    def get_config(cls: Type[A], type_: Type[C]) -> C:
        """
        ...
        """

        cfg = next((c for c in cls.configs if type(c) is type_), None)

        if cfg is None:
            msg = f"Configuration {type_} for {cls.__name__} not found"
            logger.error(msg)
            raise ValueError(msg)

        logger.debug(f"Configuration for {cls.__name__}: {type(cfg)}")

        return cfg

    # ....................... #

    def __init_subclass__(cls: Type[A], **kwargs):
        super().__init_subclass__(**kwargs)

        cls._update_ignored_types()
        cls._merge_configs()
        # cls._register_subclass()
        # cls._merge_registry()

    # ....................... #

    @classmethod
    def _update_ignored_types(cls: Type[A]):
        """
        Update ignored types for the model configuration
        """

        ignored_types = cls.model_config.get("ignored_types", tuple())

        if cls._config_type not in ignored_types:
            ignored_types += (cls._config_type,)

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
            if getattr(p, "_config_type", None) is not None:
                cfgs = p.configs  # type: ignore
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
            if getattr(p, "_config_type", None) is not None:
                reg = p._registry  # type: ignore

        logger.debug(f"Parent registry for {cls.__name__}: {reg}")
        logger.debug(f"Self registry for {cls.__name__}: {cls._registry}")

        cls._registry = cls._merge_registry_helper(reg, cls._registry)
