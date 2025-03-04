from threading import Lock
from typing import Any, Optional, Type, TypeVar

from ormy.exceptions import InternalError

from .config import ConfigABC

# ----------------------- #

C = TypeVar("C", bound=ConfigABC)

# ----------------------- #


class Registry:
    """Registry for all subclasses"""

    _registry: dict[str, Any] = {}
    _lock = Lock()

    # ....................... #

    @classmethod
    def get(cls) -> dict[str, Any]:
        """Retrieve the registry"""

        with cls._lock:
            return cls._registry

    # ....................... #

    @classmethod
    def get_by_config(cls, config: Type[C]):
        """Get registry items by config"""

        with cls._lock:
            entries = cls._registry.get(config.__name__, {})

            if entries:
                return cls.get_deepest_values(entries)

            return []

    # ....................... #

    @classmethod
    def get_deepest_values(cls, d: Any):
        """Get deepest values from a dictionary"""

        if isinstance(d, dict):
            if all(not isinstance(v, dict) for v in d.values()):
                return list(d.values())  # Return values if no nested dicts exist
            else:
                result = []
                for value in d.values():
                    result.extend(cls.get_deepest_values(value))
                return result

        return [d]

    # ....................... #

    @classmethod
    def exists(cls, discriminator: str | list[str], config: Optional[C] = None):
        """
        Check if the item exists in the registry

        Args:
            discriminator (str | list[str]): Discriminator
            config (ConfigABC): Configuration
            logger (logging.Logger): Logger

        Returns:
            exists (bool): True if the item exists, False otherwise
        """

        exists = False

        with cls._lock:
            if not isinstance(discriminator, (list, tuple, set)):
                discriminator = [discriminator]

            else:
                discriminator = list(discriminator)

            if config is None:
                raise InternalError("config is None")

            keys = []

            for d in discriminator:
                if not hasattr(config, d):
                    raise InternalError(f"Discriminator {d} not found in {config}")

                keys.append(getattr(config, d))

            retrieve = cls._registry.get(type(config).__name__, {})

            for k in keys:
                retrieve = retrieve.get(k, {})

            if retrieve:
                exists = True

        return exists

    # ....................... #

    @classmethod
    def register(
        cls,
        discriminator: str | list[str],
        value: Any,
        config: Optional[C] = None,
    ):
        """
        Register a subclass

        Args:
            discriminator (str | list[str]): Discriminator
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
                raise InternalError("config is None")

            keys = []

            for d in discriminator:
                if not hasattr(config, d):
                    raise InternalError(f"Discriminator {d} not found in {config}")

                keys.append(getattr(config, d))

            if not config.is_default():
                current = cls._registry.get(type(config).__name__, {})

                root = current

                for i, k in enumerate(keys[:-1]):
                    if k not in current:
                        current[k] = {}

                    current = current[k]

                current[keys[-1]] = value
                cls._registry[type(config).__name__] = root
