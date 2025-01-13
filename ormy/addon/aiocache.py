import asyncio
import functools
import json
import re
import time
from abc import abstractmethod
from typing import Any, Dict, List, Optional

import anyio
from aiocache import Cache as AioCache  # type: ignore[import-untyped]
from aiocache import cached as aiocache_cached  # type: ignore[import-untyped]
from aiocache.backends.redis import (  # type: ignore[import-untyped]
    RedisCache as AiocacheRedisCache,
)
from aiocache.base import API  # type: ignore[import-untyped]
from aiocache.base import BaseCache as AioBaseCache  # type: ignore[import-untyped]
from aiocache.decorators import logger  # type: ignore[import-untyped]
from aiocache.factory import caches  # type: ignore[import-untyped]

from ormy.base.error import InternalError

# ----------------------- #


class BaseCache(AioBaseCache):
    @API.register
    @API.aiocache_enabled(fake_return=True)
    @API.timeout
    @API.plugins
    async def clear(
        self,
        namespace: Optional[str] = None,
        _conn: Optional[Any] = None,
        patterns: Optional[List[str]] = None,
        except_keys: Optional[List[str]] = None,
        except_patterns: Optional[List[str]] = None,
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
        patterns: Optional[List[str]] = None,
        except_keys: Optional[List[str]] = None,
        except_patterns: Optional[List[str]] = None,
    ):
        raise NotImplementedError()


# ....................... #


class RedisCache(AiocacheRedisCache, BaseCache):
    async def _clear(
        self,
        namespace: Optional[str] = None,
        _conn: Optional[Any] = None,
        patterns: Optional[List[str]] = None,
        except_keys: Optional[List[str]] = None,
        except_patterns: Optional[List[str]] = None,
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


class CustomCache(AioCache):
    REDIS = RedisCache


# ....................... #


class _cached(aiocache_cached):
    """
    Subclass of `.aiocache_cached` decorator that supports synchronous functions.
    """

    def __call__(self, f):
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

        if asyncio.iscoroutinefunction(f):

            @functools.wraps(f)
            async def async_wrapper(*args, **kwargs):
                return await self.decorator(f, *args, **kwargs)

            async_wrapper.cache = self.cache  # type: ignore
            return async_wrapper

        else:

            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                return anyio.run(self.decorator, f, *args, **kwargs)

            wrapper.cache = self.cache  # type: ignore
            return wrapper

    # ....................... #

    async def decorator(
        self,
        f,
        *args,
        cache_read=True,
        cache_write=True,
        aiocache_wait_for_write=True,
        **kwargs,
    ):
        key = self.get_cache_key(f, args, kwargs)

        if cache_read:
            value = await self.get_from_cache(key)
            if value is not None:
                return value

        if asyncio.iscoroutinefunction(f):
            result = await f(*args, **kwargs)

        else:
            result = f(*args, **kwargs)

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


def generate_pattern(criteria: Dict[str, Any]):
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


def _key_factory(
    name: str,
    include_params: Optional[List[str]] = None,
):
    """
    Create a cache key for a function.

    Args:
        name (str): A name to use.
        include_params (List[str], optional): A list of keys to use as the cache key.

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

    def key_builder(f, *args, **kwargs):
        arg_names: List[str] = f.__code__.co_varnames[: f.__code__.co_argcount]
        defaults = f.__defaults__ or ()
        pos_defaults = dict(zip(arg_names[-len(defaults) :], defaults))
        key_dict: Dict[str, Any] = {"name": name}
        self_or_cls = args[0]

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

_ID_ALIASES = ["_id", "id_"]


def _extract_namespace(target_entity, target_id, func, self_or_cls, *args, **kwargs):
    if target_id is None:
        arg_names: List[str] = func.__code__.co_varnames[: func.__code__.co_argcount][
            1:
        ]
        id_arg_name = next((n for n in arg_names if n in _ID_ALIASES), None)

        if id_arg_name is None:
            if not hasattr(self_or_cls, "id"):
                raise InternalError(f"`{func.__name__}` does not have an id argument")

            _id = getattr(self_or_cls, "id")

        else:
            if id_arg_name in kwargs.keys():
                _id = kwargs[id_arg_name]

            else:
                _id = args[arg_names.index(id_arg_name)]

    else:
        _id = target_id

    if target_entity is None:
        if hasattr(self_or_cls, "_get_entity") and callable(self_or_cls._get_entity):
            namespace = self_or_cls._get_entity()

        else:
            raise InternalError(f"{self_or_cls} does not have a '_get_entity' method")

    else:
        namespace = target_entity

    return namespace, _id


# ....................... #


def cache(
    name: str,
    include_params: Optional[List[str]] = None,
    **cache_kwargs,
):
    """
    Decorator to cache a function result.

    Args:
        plain (Optional[str]): The plain string to use as the cache key.
        from_keys (Optional[List[str]]): The keys to use as the cache key.
        **cache_kwargs: The cache kwargs.

    Returns:
        decorator (Callable): The decorator to cache a function.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(self_or_cls, *args, **kwargs):
            namespace, _id = _extract_namespace(
                None,
                None,
                func,
                self_or_cls,
                *args,
                **kwargs,
            )

            cache_kwargs["key_builder"] = _key_factory(name, include_params)
            cache_kwargs["namespace"] = f"{namespace}:{_id}"

            return _cached(**cache_kwargs)(func)(self_or_cls, *args, **kwargs)

        return wrapper

    return decorator


# ....................... #


def inline_cache_clear(
    func,
    self_or_cls,
    *args,
    target_entity: Optional[str] = None,
    target_id: Optional[str] = None,
    keys: Optional[List[str]] = None,
    patterns: Optional[List[str]] = None,
    except_keys: Optional[List[str]] = None,
    except_patterns: Optional[List[str]] = None,
    cache_kwargs: Dict[str, Any] = {},
    **kwargs,
):
    """
    Function to clear the cache

    Args:
        func (Callable): The function to clear the cache for.
        self_or_cls (Any): The self or cls to clear the cache for.
        target_entity (str, optional): The entity to clear the cache for.
        target_id (str, optional): The id to clear the cache for.
        keys (List[str], optional): The keys to clear the cache for.
        patterns (List[str], optional): The patterns to clear the cache for.
        except_keys (List[str], optional): The keys to exclude from the cache clear.
        except_patterns (Optional[List[str]]): The patterns to exclude from the cache clear.
    """

    namespace, _id = _extract_namespace(
        target_entity,
        target_id,
        func,
        self_or_cls,
        *args,
        **kwargs,
    )

    cache_kwargs["namespace"] = f"{namespace}:{_id}"
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
                patterns,
                except_keys,
                except_patterns,
            )  # type: ignore

        else:
            anyio.run(
                cache.clear,
                cache.namespace,
            )  # type: ignore


# ....................... #


async def ainline_cache_clear(
    func,
    self_or_cls,
    *args,
    target_entity: Optional[str] = None,
    target_id: Optional[str] = None,
    keys: Optional[List[str]] = None,
    patterns: Optional[List[str]] = None,
    except_keys: Optional[List[str]] = None,
    except_patterns: Optional[List[str]] = None,
    cache_kwargs: Dict[str, Any] = {},
    **kwargs,
):
    """
    Function to clear the cache

    Args:
        func (Callable): The function to clear the cache for.
        self_or_cls (Any): The self or cls to clear the cache for.
        target_entity (str, optional): The entity to clear the cache for.
        target_id (str, optional): The id to clear the cache for.
        keys (List[str], optional): The keys to clear the cache for.
        patterns (List[str], optional): The patterns to clear the cache for.
        except_keys (List[str], optional): The keys to exclude from the cache clear.
        except_patterns (List[str], optional): The patterns to exclude from the cache clear.
    """

    namespace, _id = _extract_namespace(
        target_entity,
        target_id,
        func,
        self_or_cls,
        *args,
        **kwargs,
    )

    cache_kwargs["namespace"] = f"{namespace}:{_id}"
    cache = CustomCache(**cache_kwargs)

    if keys:
        for k in keys:
            await cache.delete(k, cache.namespace)  # type: ignore

    else:
        if cache_kwargs["cache_class"] is CustomCache.REDIS:
            await cache.clear(
                namespace=cache.namespace,
                patterns=patterns,  # type: ignore
                except_keys=except_keys,  # type: ignore
                except_patterns=except_patterns,  # type: ignore
            )

        else:
            await cache.clear(namespace=cache.namespace)  # type: ignore


# ....................... #


def cache_clear(
    target_entity: Optional[str] = None,
    target_id: Optional[str] = None,
    keys: Optional[List[str]] = None,
    patterns: Optional[List[str]] = None,
    except_keys: Optional[List[str]] = None,
    except_patterns: Optional[List[str]] = None,
    **cache_kwargs,
):
    """
    Decorator to clear the cache

    Args:
        target_entity (str, optional): The entity to clear the cache for.
        target_id (str, optional): The id to clear the cache for.
        keys (List[str], optional): The keys to clear the cache for.
        patterns (List[str], optional): The patterns to clear the cache for.
        except_keys (List[str], optional): The keys to exclude from the cache clear.
        except_patterns (List[str], optional): The patterns to exclude from the cache clear.

    Returns:
        decorator (Callable): The decorator to clear the cache.
    """

    def decorator(func):
        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(self_or_cls, *args, **kwargs):
                res = await func(self_or_cls, *args, **kwargs)

                await ainline_cache_clear(
                    func,
                    self_or_cls,
                    *args,
                    target_entity=target_entity,
                    target_id=target_id,
                    keys=keys,
                    patterns=patterns,
                    except_keys=except_keys,
                    except_patterns=except_patterns,
                    cache_kwargs=cache_kwargs,
                    **kwargs,
                )

                return res

            return async_wrapper

        else:

            @functools.wraps(func)
            def sync_wrapper(self_or_cls, *args, **kwargs):
                res = func(self_or_cls, *args, **kwargs)

                inline_cache_clear(
                    func,
                    self_or_cls,
                    *args,
                    target_entity=target_entity,
                    target_id=target_id,
                    keys=keys,
                    patterns=patterns,
                    except_keys=except_keys,
                    except_patterns=except_patterns,
                    cache_kwargs=cache_kwargs,
                    **kwargs,
                )

                return res

            return sync_wrapper

    return decorator


# ----------------------- #

__all__ = [
    "cache",
    "cache_clear",
    "ainline_cache_clear",
    "inline_cache_clear",
]
