import asyncio
import functools
import json
import re
import time
from abc import abstractmethod
from typing import Any, Callable, Optional, ParamSpec, TypeVar

import anyio

from ormy.base.typing import AsyncCallable
from ormy.exceptions import InternalError

# ----------------------- #

P = ParamSpec("P")
T = TypeVar("T")

# ----------------------- #


def _get_api():
    """Dynamically import API only when needed."""
    try:
        from aiocache.base import API  # type: ignore[import-untyped]
    except ImportError:
        raise ImportError(
            "Aiocache dependency is missing. Install with `pip install ormy[cache]`."
        )
    return API


# ....................... #


def _get_cache():
    """Dynamically import AioCache only when needed."""
    try:
        from aiocache import Cache  # type: ignore[import-untyped]
    except ImportError:
        raise ImportError(
            "Aiocache dependency is missing. Install with `pip install ormy[cache]`."
        )
    return Cache


# ....................... #


def _get_cached():
    """Dynamically import cached only when needed."""
    try:
        from aiocache.decorators import cached  # type: ignore[import-untyped]
    except ImportError:
        raise ImportError(
            "Aiocache dependency is missing. Install with `pip install ormy[cache]`."
        )

    return cached


# ....................... #


def _get_redis_cache():
    """Dynamically import RedisCache only when needed."""
    try:
        from aiocache.backends.redis import RedisCache  # type: ignore[import-untyped]
    except ImportError:
        raise ImportError(
            "Aiocache dependency is missing. Install with `pip install ormy[cache]`."
        )

    return RedisCache


# ....................... #


def _get_base_cache():
    """Dynamically import BaseCache only when needed."""
    try:
        from aiocache.base import BaseCache  # type: ignore[import-untyped]
    except ImportError:
        raise ImportError(
            "Aiocache dependency is missing. Install with `pip install ormy[cache]`."
        )

    return BaseCache


# ....................... #


class BaseCache(_get_base_cache()):  # type: ignore[misc]
    @_get_api().register  # type: ignore[misc]
    @_get_api().aiocache_enabled(fake_return=True)  # type: ignore[misc]
    @_get_api().timeout  # type: ignore[misc]
    @_get_api().plugins  # type: ignore[misc]
    async def clear(
        self,
        namespace: Optional[str] = None,
        _conn: Optional[Any] = None,
        patterns: Optional[list[str]] = None,
        except_keys: Optional[list[str]] = None,
        except_patterns: Optional[list[str]] = None,
    ):
        """
        Clears the cache in the cache namespace. If an alternative namespace is given, it will
        clear those ones instead.

        :param namespace: str alternative namespace to use
        :param timeout: int or float in seconds specifying maximum timeout
            for the operations to last
        :returns: True
        :raises: :class:`asyncio.TimeoutError` if it lasts more than self.timeout
        """

        from aiocache.decorators import logger

        start = time.monotonic()
        ret = await self._clear(
            namespace,
            _conn=_conn,
            patterns=patterns,
            except_keys=except_keys,
            except_patterns=except_patterns,
        )
        logger.debug("CLEAR %s %d (%.4f)s", namespace, ret, time.monotonic() - start)
        return ret

    # ....................... #

    @abstractmethod
    async def _clear(
        self,
        namespace: Optional[str] = None,
        _conn: Optional[Any] = None,
        patterns: Optional[list[str]] = None,
        except_keys: Optional[list[str]] = None,
        except_patterns: Optional[list[str]] = None,
    ):
        raise NotImplementedError()


# ....................... #


class RedisCache(_get_redis_cache(), BaseCache):  # type: ignore[misc]
    async def _clear(
        self,
        namespace: Optional[str] = None,
        _conn: Optional[Any] = None,
        patterns: Optional[list[str]] = None,
        except_keys: Optional[list[str]] = None,
        except_patterns: Optional[list[str]] = None,
    ):
        if namespace:
            keys = await self.client.keys("{}:*".format(namespace))

            print("!!!!!!", [k.decode() for k in keys])

            if patterns:
                keys = [
                    k
                    for k in keys
                    if any(
                        re.search(p, ":".join(k.decode().split(":")[1:]))
                        for p in patterns
                    )
                ]

                print("!!!!!! Patterns", [k.decode() for k in keys])

            if except_keys:
                keys = [
                    k
                    for k in keys
                    if ":".join(k.decode().split(":")[1:]) not in except_keys
                ]

                print("!!!!!! Except keys", [k.decode() for k in keys])

            if except_patterns:
                keys = [
                    k
                    for k in keys
                    if not any(
                        re.search(p, ":".join(k.decode().split(":")[1:]))
                        for p in except_patterns
                    )
                ]

                print("!!!!!! Except patterns", [k.decode() for k in keys])

            if keys:
                await self.client.delete(*keys)

        else:
            await self.client.flushdb()

        return True


# ....................... #


class CustomCache(_get_cache()):  # type: ignore[misc]
    REDIS = RedisCache


# ....................... #


class _acached(_get_cached()):  # type: ignore[misc]
    """
    Subclass of `aiocache_cached` decorator that supports synchronous functions.
    """

    def __call__(self, f: AsyncCallable[P, T]):
        from aiocache.decorators import logger  # type: ignore[import-untyped]
        from aiocache.factory import caches  # type: ignore[import-untyped]

        if self.alias:
            self.cache = caches.get(self.alias)  #! ???
            for arg in ("serializer", "namespace", "plugins"):
                if getattr(self, f"_{arg}", None) is not None:
                    logger.warning(f"Using cache alias; ignoring '{arg}' argument.")
        else:
            self.cache = CustomCache(
                cache_class=self._cache,
                serializer=self._serializer,
                namespace=self._namespace,
                plugins=self._plugins,
                **self._kwargs,
            )

        @functools.wraps(f)
        async def wrapper(*args, **kwargs) -> T:
            return await self.decorator(f, *args, **kwargs)

        wrapper.cache = self.cache  # type: ignore
        return wrapper

    # ....................... #

    async def decorator(
        self,
        f: AsyncCallable[P, T],
        *args,
        cache_read: bool = True,
        cache_write: bool = True,
        aiocache_wait_for_write: bool = True,
        **kwargs,
    ) -> T:
        key = self.get_cache_key(f, args, kwargs)

        if cache_read:
            value: T | None = await self.get_from_cache(key)

            if value is not None:
                return value

        result: T = await f(*args, **kwargs)

        if self.skip_cache_func(result):
            return result

        if cache_write:
            if aiocache_wait_for_write:
                await self.set_in_cache(key, result)
            else:
                # TODO: Use aiojobs to avoid warnings.
                asyncio.create_task(self.set_in_cache(key, result))

        return result


# ....................... #


class _cached(_get_cached()):  # type: ignore[misc]
    """
    Subclass of `aiocache_cached` decorator that supports synchronous functions.
    """

    def __call__(self, f: Callable[P, T]):
        from aiocache.decorators import logger
        from aiocache.factory import caches

        if self.alias:
            self.cache = caches.get(self.alias)  #! ???
            for arg in ("serializer", "namespace", "plugins"):
                if getattr(self, f"_{arg}", None) is not None:
                    logger.warning(f"Using cache alias; ignoring '{arg}' argument.")
        else:
            self.cache = CustomCache(
                cache_class=self._cache,
                serializer=self._serializer,
                namespace=self._namespace,
                plugins=self._plugins,
                **self._kwargs,
            )

        @functools.wraps(f)
        def wrapper(*args, **kwargs) -> T:
            return anyio.run(self.decorator, f, *args, **kwargs)  # type: ignore

        wrapper.cache = self.cache  # type: ignore
        return wrapper

    # ....................... #

    async def decorator(
        self,
        f: Callable[P, T],
        *args,
        cache_read: bool = True,
        cache_write: bool = True,
        aiocache_wait_for_write: bool = True,
        **kwargs,
    ) -> T:
        key = self.get_cache_key(f, args, kwargs)

        if cache_read:
            value: T | None = await self.get_from_cache(key)

            if value is not None:
                return value

        result: T = f(*args, **kwargs)

        if self.skip_cache_func(result):
            return result

        if cache_write:
            if aiocache_wait_for_write:
                await self.set_in_cache(key, result)
            else:
                # TODO: Use aiojobs to avoid warnings.
                asyncio.create_task(self.set_in_cache(key, result))

        return result


# ....................... #


def generate_pattern(criteria: dict[str, Any]):
    """
    Generate a regex pattern to match all key-value pairs in the given dictionary.

    Args:
        criteria (dict): A dictionary where keys are parameters and values are expected values.

    Returns:
        pattern (str): A regex pattern string.
    """

    patterns = [
        rf"(?=.*(?:^|;){re.escape(k)}={re.escape(str(v))}(?:;|$))"
        for k, v in criteria.items()
    ]
    # Combine all patterns into one
    return "".join(patterns)


# ....................... #

_ID_ALIASES = ["_id", "id_"]


def _parse_f_signature(f: Callable | AsyncCallable, *args, **kwargs):
    arg_names = f.__code__.co_varnames[: f.__code__.co_argcount]
    defaults = f.__defaults__ or ()
    pos_defaults = dict(zip(arg_names[-len(defaults) :], defaults))
    self_or_cls = args[0]

    id_arg_name = next((n for n in arg_names if n in _ID_ALIASES), None)

    if id_arg_name is None:
        if not hasattr(self_or_cls, "id"):
            raise InternalError(f"`{f.__name__}` does not have an id argument")

        _id = getattr(self_or_cls, "id")

    else:
        if id_arg_name in kwargs.keys():
            _id = kwargs[id_arg_name]

        else:
            _id = args[arg_names.index(id_arg_name)]

    return _id, pos_defaults, arg_names, self_or_cls


# ....................... #


def _key_factory(
    name: str,
    include_params: Optional[list[str]] = None,
):
    """
    Create a cache key for a function.

    Args:
        name (str): A name to use.
        include_params (list[str], optional): A list of keys to use as the cache key.

    Returns:
        key_builder (Callable): A function that creates a cache key for a function.
    """

    _as_is_tuple = (str, int, float, bool, list, dict, tuple, set)

    def _safe_dump(v: Any):
        """
        Safely dump a value to a string.
        """

        if not isinstance(v, _as_is_tuple):
            v = json.dumps(v, sort_keys=True, default=str)

        return v

    def key_builder(f: Callable | AsyncCallable, *args, **kwargs):
        _id, pos_defaults, arg_names, self_or_cls = _parse_f_signature(
            f, *args, **kwargs
        )
        key_dict: dict[str, Any] = {"name": name, "id": _id}

        if include_params:
            for u in include_params:
                if u.startswith("self."):
                    attr = u.split(".")[1]
                    key_dict[attr] = _safe_dump(getattr(self_or_cls, attr))

                elif u in kwargs:
                    key_dict[u] = _safe_dump(kwargs[u])

                elif len(args) > arg_names.index(u):
                    key_dict[u] = _safe_dump(args[arg_names.index(u)])

                else:
                    key_dict[u] = pos_defaults[u]

        return ";".join([f"{k}={v}" for k, v in key_dict.items()])

    return key_builder


# ....................... #


def _extract_namespace(self_or_cls):
    if hasattr(self_or_cls, "_get_entity") and callable(self_or_cls._get_entity):
        namespace = self_or_cls._get_entity()
        return namespace

    raise InternalError(f"{self_or_cls} does not have a '_get_entity' method")


# ....................... #


def acache(
    name: str,
    include_params: Optional[list[str]] = None,
    **cache_kwargs,
):
    """
    Decorator to cache a function result.

    Args:
        name (str): The name to use in the cache key.
        include_params (list[str], optional): The parameters to use in the cache key.
        **cache_kwargs: The cache kwargs.

    Returns:
        decorator (Callable): The decorator to cache a function.
    """

    def decorator(func: AsyncCallable[P, T]):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            namespace = _extract_namespace(args[0])
            cache_kwargs["key_builder"] = _key_factory(name, include_params)
            cache_kwargs["namespace"] = namespace

            return _acached(**cache_kwargs)(func)(*args, **kwargs)

        return wrapper

    return decorator


# ....................... #


def cache(
    name: str,
    include_params: Optional[list[str]] = None,
    **cache_kwargs,
):
    """
    Decorator to cache a function result.

    Args:
        name (str): The name to use in the cache key.
        include_params (list[str], optional): The parameters to use in the cache key.
        **cache_kwargs: The cache kwargs.

    Returns:
        decorator (Callable): The decorator to cache a function.
    """

    def decorator(func: Callable[P, T]):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            namespace = _extract_namespace(args[0])
            cache_kwargs["key_builder"] = _key_factory(name, include_params)
            cache_kwargs["namespace"] = namespace

            return _cached(**cache_kwargs)(func)(*args, **kwargs)

        return wrapper

    return decorator


# ....................... #


def inline_cache_clear(
    namespace: str,
    keys: Optional[list[str]] = None,
    patterns: Optional[list[dict[str, Any]]] = None,
    except_keys: Optional[list[str]] = None,
    except_patterns: Optional[list[dict[str, Any]]] = None,
    **cache_kwargs,
):
    """
    Function to clear the cache

    Args:
        namespace (str): The namespace to clear the cache for.
        keys (list[str], optional): The keys to clear the cache for.
        patterns (list[dict[str, Any]], optional): The patterns to clear the cache for.
        except_keys (list[str], optional): The keys to exclude from the cache clear.
        except_patterns (list[dict[str, Any]], optional): The patterns to exclude from the cache clear.
    """

    if patterns is not None:
        plain_patterns = [generate_pattern(p) for p in patterns]

    else:
        plain_patterns = None

    if except_patterns is not None:
        plain_except_patterns = [generate_pattern(p) for p in except_patterns]

    else:
        plain_except_patterns = None

    cache_kwargs["namespace"] = namespace
    cache = CustomCache(**cache_kwargs)

    if keys:
        for k in keys:
            anyio.run(
                cache.delete,
                k,
                cache.namespace,
            )  # type: ignore

    else:
        if cache_kwargs["cache_class"] is CustomCache.REDIS:
            anyio.run(
                cache.clear,
                cache.namespace,
                None,
                plain_patterns,
                except_keys,
                plain_except_patterns,
            )  # type: ignore

        else:
            anyio.run(
                cache.clear,
                cache.namespace,
            )  # type: ignore


# ....................... #


async def ainline_cache_clear(
    namespace: str,
    keys: Optional[list[str]] = None,
    patterns: Optional[list[dict[str, Any]]] = None,
    except_keys: Optional[list[str]] = None,
    except_patterns: Optional[list[dict[str, Any]]] = None,
    **cache_kwargs,
):
    """
    Function to clear the cache

    Args:
        namespace (str): The namespace to clear the cache for.
        keys (list[str], optional): The keys to clear the cache for.
        patterns (list[dict[str, Any]], optional): The patterns to clear the cache for.
        except_keys (list[str], optional): The keys to exclude from the cache clear.
        except_patterns (list[dict[str, Any]], optional): The patterns to exclude from the cache clear.
    """

    if patterns is not None:
        plain_patterns = [generate_pattern(p) for p in patterns]

    else:
        plain_patterns = None

    if except_patterns is not None:
        plain_except_patterns = [generate_pattern(p) for p in except_patterns]

    else:
        plain_except_patterns = None

    cache_kwargs["namespace"] = namespace
    cache = CustomCache(**cache_kwargs)

    if keys:
        for k in keys:
            await cache.delete(k, cache.namespace)  # type: ignore

    else:
        if cache_kwargs["cache_class"] is CustomCache.REDIS:
            await cache.clear(
                namespace=cache.namespace,
                patterns=plain_patterns,  # type: ignore
                except_keys=except_keys,  # type: ignore
                except_patterns=plain_except_patterns,  # type: ignore
            )

        else:
            await cache.clear(namespace=cache.namespace)  # type: ignore


# ....................... #


def acache_clear(
    keys: Optional[list[str]] = None,
    patterns: Optional[list[dict[str, Any]]] = None,
    except_keys: Optional[list[str]] = None,
    except_patterns: Optional[list[dict[str, Any]]] = None,
    **cache_kwargs,
):
    """
    Decorator to clear the cache

    Args:
        keys (list[str], optional): The keys to clear the cache for.
        patterns (list[dict[str, Any]], optional): The patterns to clear the cache for.
        except_keys (list[str], optional): The keys to exclude from the cache clear.
        except_patterns (list[dict[str, Any]], optional): The patterns to exclude from the cache clear.

    Returns:
        decorator (Callable): The decorator to clear the cache.
    """

    def decorator(func: AsyncCallable[P, T]):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            try:
                _id, _, _, _ = _parse_f_signature(func, *args, **kwargs)

            # TODO: replace with native error
            except Exception as e:
                print(f"Failed to extract id from function signature: {e}")
                _id = None

            res: T = await func(*args, **kwargs)

            if _id is not None:
                if patterns:
                    upd_patterns = [{**x, "id": _id} for x in patterns]

                else:
                    upd_patterns = [{"id": _id}]

            else:
                upd_patterns = patterns  # type: ignore[assignment]

            await ainline_cache_clear(
                namespace=_extract_namespace(args[0]),
                keys=keys,
                patterns=upd_patterns,
                except_keys=except_keys,
                except_patterns=except_patterns,
                **cache_kwargs,
            )

            return res

        return wrapper

    return decorator


# ....................... #


def cache_clear(
    keys: Optional[list[str]] = None,
    patterns: Optional[list[dict[str, Any]]] = None,
    except_keys: Optional[list[str]] = None,
    except_patterns: Optional[list[dict[str, Any]]] = None,
    **cache_kwargs,
):
    """
    Decorator to clear the cache

    Args:
        keys (list[str], optional): The keys to clear the cache for.
        patterns (list[dict[str, Any]], optional): The patterns to clear the cache for.
        except_keys (list[str], optional): The keys to exclude from the cache clear.
        except_patterns (list[dict[str, Any]], optional): The patterns to exclude from the cache clear.

    Returns:
        decorator (Callable): The decorator to clear the cache.
    """

    def decorator(func: Callable[P, T]):
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            try:
                _id, _, _, _ = _parse_f_signature(func, *args, **kwargs)

            # TODO: replace with native error
            except Exception as e:
                print(f"Failed to extract id from function signature: {e}")
                _id = None

            res: T = func(*args, **kwargs)  # type: ignore

            if _id is not None:
                if patterns:
                    upd_patterns = [{**x, "id": _id} for x in patterns]

                else:
                    upd_patterns = [{"id": _id}]

            else:
                upd_patterns = patterns  # type: ignore[assignment]

            inline_cache_clear(
                namespace=_extract_namespace(args[0]),
                keys=keys,
                patterns=upd_patterns,
                except_keys=except_keys,
                except_patterns=except_patterns,
                **cache_kwargs,
            )

            return res

        return wrapper

    return decorator


# ----------------------- #

__all__ = [
    "cache",
    "acache",
    "cache_clear",
    "acache_clear",
    "ainline_cache_clear",
    "inline_cache_clear",
]
