from .config import MongoConfig, MongoCredentials
from .wrapper import MongoBase

# ----------------------- #

__all__ = [
    "MongoConfig",
    "MongoCredentials",
    "MongoBase",
]

# # Expose only relevant classes in import *
# __all__ = get_subclass_names(locals(), Operation)

# def get_subclass_names(locals, base_class):
#     from inspect import isclass
#     return [c.__name__ for c in locals.values() if isclass(c) and issubclass(c, base_class)]
