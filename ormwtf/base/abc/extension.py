# from typing import Any, ClassVar, Dict, Type, TypeVar

# from ormwtf.utils.logging import LogLevel, console_logger

# from .abstract import AbstractABC
# from .config import ConfigABC

# # ----------------------- #

# E = TypeVar("E", bound="ExtensionABC")
# C = TypeVar("C", bound="ConfigABC")

# logger = console_logger(__name__, level=LogLevel.DEBUG)

# # ....................... #


# class ExtensionABC(AbstractABC):
#     """
#     Abstract Base Class for Extension
#     """

#     ext_config: ClassVar[Dict[str, C]] = dict()
#     ext_registry: ClassVar[Dict[str, Any]] = dict()

#     # ....................... #

#     def __init_subclass__(cls: Type[E], **kwargs):
#         super().__init_subclass__(**kwargs)

#         for x in cls.ext_config.values():
#             cls.update_ignored_types(type_=type(x))
