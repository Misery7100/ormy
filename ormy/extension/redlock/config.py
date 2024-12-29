from ormy.service.redis import RedisConfig, RedisCredentials

# ----------------------- #


class RedlockCredentials(RedisCredentials):
    """
    Redis connect credentials for Redlock

    Attributes:
        host (str): Redis host
        port (int): Redis port
        username (SecretStr): Redis username (applicable only for Redis with ACL)
        password (SecretStr): Redis password
    """

    pass


# ....................... #


class RedlockConfig(RedisConfig):
    """
    Configuration for Redlock extension

    Attributes:
        database (int): Database number to assign
        collection (str): Collection name to assign
        credentials (RedlockCredentials): Connection credentials
        use_static (bool): Whether to use static Redis client
    """

    # Global configuration
    credentials: RedlockCredentials = RedlockCredentials()
    use_static: bool = False
