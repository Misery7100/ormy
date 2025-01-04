import logging
from threading import Lock
from typing import Any, Dict, List, Optional, TypeVar

from ormy.base.error import InternalError
from ormy.base.logging import LogManager

from .config import ConfigABC

# ----------------------- #

C = TypeVar("C", bound=ConfigABC)
logger = LogManager.get_logger(__name__)

# ----------------------- #


class Registry:
    """Registry for all subclasses"""

    _registry: Dict[str, Any] = {}
    _lock = Lock()

    # ....................... #

    @classmethod
    def retrieve(cls) -> Dict[str, Any]:
        """Retrieve the registry"""

        with cls._lock:
            return cls._registry

    # ....................... #

    @classmethod
    def register(
        cls,
        discriminator: str | List[str],
        value: Any,
        config: Optional[C] = None,
        logger: logging.Logger = logger,
    ):
        """
        Register a subclass

        Args:
            discriminator (str | List[str]): Discriminator
            value (Any): Value
            config (ConfigABC): Configuration
            logger (logging.Logger): Logger
        """

        with cls._lock:
            if not isinstance(discriminator, (list, tuple, set)):
                discriminator = [discriminator]

            else:
                discriminator = list(discriminator)

            if config is None:
                msg = "Config is None"
                logger.error(msg)

                raise InternalError(msg)

            keys = []

            for d in discriminator:
                if not hasattr(config, d):
                    msg = f"Discriminator {d} not found in {config}"
                    logger.error(msg)

                    raise InternalError(msg)

                keys.append(getattr(config, d))

            if not config.is_default():
                logger.debug(
                    f"Adding subclass {value} for `{type(config).__name__}` using discriminator: {keys}"
                )
                logger.debug(f"Registry before: {cls._registry}")

                current = cls._registry.get(type(config).__name__, {})

                root = current

                for i, k in enumerate(keys[:-1]):
                    if k not in current:
                        current[k] = {}

                    current = current[k]

                current[keys[-1]] = cls
                cls._registry[type(config).__name__] = root

                logger.debug(f"Registry after: {cls._registry}")
