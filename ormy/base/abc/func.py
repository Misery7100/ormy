from logging import Logger
from typing import List, TypeVar

from ormy.base.error import InternalError
from ormy.utils.logging import LogLevel, console_logger

from .config import ConfigABC

# ----------------------- #

C = TypeVar("C", bound=ConfigABC)

logger = console_logger(__name__, level=LogLevel.INFO)

# ----------------------- #


def register_subclass(
    cls,
    config: C,  # type: ignore
    discriminator: str | List[str],
    logger: Logger = logger,
):
    """
    Register subclass in the registry

    Args:
        cls (Any): Subclass
        config (C): Configuration
        discriminator (str): Discriminator
    """

    if isinstance(discriminator, str):
        discriminator = [discriminator]

    keys = []

    for d in discriminator:
        if not hasattr(config, d):
            msg = f"Discriminator {d} not found in {config}"
            logger.error(msg)

            raise InternalError(msg)

        keys.append(getattr(config, d))

    if config.include_to_registry and not config.is_default():
        logger.debug(f"Registering {cls.__name__} using {keys}")
        logger.debug(f"Registry before: {cls._registry}")

        current = cls._registry.get(type(config), {})

        for k in keys[:-1]:
            if k not in current:
                current[k] = {}

            current = current[k]

        current[keys[-1]] = cls
        cls._registry[type(config)] = current

        logger.debug(f"Registry after: {cls._registry}")


# ....................... #


def merge_registry_helper(
    cls,
    d1: dict,
    d2: dict,
    logger: Logger = logger,
) -> dict:
    """
    Merge registry for the subclass

    Args:
        d1 (dict): First registry
        d2 (dict): Second registry

    Returns:
        merged (dict): Merged registry
    """

    for k in d2.keys():
        if k in d1:
            if isinstance(d1[k], dict) and isinstance(d2[k], dict):
                merge_registry_helper(cls, d1[k], d2[k], logger=logger)

            else:
                d1[k] = d2[k]
                logger.debug(f"Overwriting {k} in registry: {d1[k]} -> {d2[k]}")

        else:
            d1[k] = d2[k]
            logger.debug(f"Adding {k} in registry: {d1[k]} -> {d2[k]}")

    return d1
