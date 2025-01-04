# from logging import Logger
# from typing import List, Optional, TypeVar

# from ormy.base.error import InternalError
# from ormy.base.logging import LogManager

# from .config import ConfigABC

# # ----------------------- #

# C = TypeVar("C", bound=ConfigABC)

# logger = LogManager.get_logger(__name__)

# # ----------------------- #

# def merge_registry_helper(
#     cls,
#     d1: dict,
#     d2: dict,
#     logger: Logger = logger,
# ) -> dict:
#     """
#     Merge registry for the subclass

#     Args:
#         d1 (dict): First registry
#         d2 (dict): Second registry

#     Returns:
#         merged (dict): Merged registry
#     """

#     for k in d2.keys():
#         if k in d1:
#             if isinstance(d1[k], dict) and isinstance(d2[k], dict):
#                 merge_registry_helper(cls, d1[k], d2[k], logger=logger)

#             else:
#                 d1[k] = d2[k]
#                 logger.debug(f"Overwriting {k} in registry: {d1[k]} -> {d2[k]}")

#         else:
#             d1[k] = d2[k]
#             logger.debug(f"Adding {k} in registry: {d1[k]} -> {d2[k]}")

#     return d1
