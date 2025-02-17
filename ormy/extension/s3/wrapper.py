import re
from contextlib import contextmanager
from typing import Any, ClassVar, Dict, List, TypeVar

from ormy.base.abc import ExtensionABC
from ormy.base.error import BadRequest, Conflict
from ormy.base.generic import TabularData
from ormy.base.pydantic import TableResponse

from .config import S3Config
from .schema import S3File

# ----------------------- #

S = TypeVar("S", bound="S3Extension")

# ----------------------- #


class S3Extension(ExtensionABC):
    """S3 extension"""

    extension_configs: ClassVar[List[Any]] = [S3Config()]

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)

        cls._register_extension_subclass_helper(
            config=S3Config,
            discriminator="bucket",
        )
        cls.__s3_create_bucket()

    # ....................... #

    @classmethod
    def _s3_get_bucket(cls) -> str:
        """Get bucket name"""

        cfg = cls.get_extension_config(type_=S3Config)
        return cfg.bucket

    # ....................... #

    @classmethod
    def __s3_create_bucket(cls):
        """Create a bucket"""

        cfg = cls.get_extension_config(type_=S3Config)

        if not cfg.is_default() and not cls._s3_bucket_exists():
            with cls.__s3_client() as client:  # type: ignore
                client.create_bucket(Bucket=cls._s3_get_bucket())

    # ....................... #

    @classmethod
    def _s3_bucket_exists(cls):
        """
        Check if a bucket exists

        Args:
            bucket (str): Bucket name

        Returns:
            result (bool): Whether the bucket exists
        """

        with cls.__s3_client() as client:  # type: ignore
            try:
                client.head_bucket(Bucket=cls._s3_get_bucket())
                return True

            except client.exceptions.ClientError as e:
                if e.response.get("Error", {}).get("Code", {}) == "404":
                    return False

                else:
                    raise e

    # ....................... #

    @classmethod
    @contextmanager
    def __s3_client(cls):
        """Get syncronous S3 client"""

        import boto3  # type: ignore[import-untyped]
        from botocore.client import Config  # type: ignore[import-untyped]

        cfg = cls.get_extension_config(type_=S3Config)
        credentials = cfg.credentials

        if credentials.username is None or credentials.password is None:
            raise BadRequest("S3 credentials are not set")

        c = boto3.client(
            service_name="s3",
            endpoint_url=credentials.url(),
            aws_access_key_id=credentials.username.get_secret_value(),
            aws_secret_access_key=credentials.password.get_secret_value(),
            config=Config(signature_version="s3v4"),
        )

        try:
            yield c

        finally:
            c.close()

    # ....................... #

    @classmethod
    def s3_list_buckets(cls):
        """List all buckets"""

        with cls.__s3_client() as client:  # type: ignore
            return client.list_buckets()

    # ....................... #

    @classmethod
    def s3_file_exists(cls, key: str):
        """
        Check if a file exists

        Args:
            key (str): File key

        Returns:
            result (bool): Whether the file exists
        """

        with cls.__s3_client() as client:  # type: ignore
            try:
                client.head_object(
                    Bucket=cls._s3_get_bucket(),
                    Key=key,
                )
                return True

            except client.exceptions.ClientError as e:
                if e.response.get("Error", {}).get("Code", {}) == "404":
                    return False

                else:
                    raise e

    # ....................... #

    @classmethod
    def s3_get_file_tags(cls, key: str) -> Dict[str, str]:
        """
        Get file tags

        Args:
            key (str): File key

        Returns:
            tags (dict): File tags
        """

        with cls.__s3_client() as client:  # type: ignore
            tagging = client.get_object_tagging(
                Bucket=cls._s3_get_bucket(),
                Key=key,
            )

            return {t["Key"]: t["Value"] for t in tagging.get("TagSet", [])}

    # ....................... #

    @classmethod
    def s3_add_file_tags(cls, key: str, tags: Dict[str, str]):
        """
        Add file tags

        Args:
            key (str): File key
            tags (dict): File tags
        """

        with cls.__s3_client() as client:  # type: ignore
            existing_tags = cls.s3_get_file_tags(key)
            merged_tags = {**existing_tags, **tags}
            new_tags = [{"Key": k, "Value": v} for k, v in merged_tags.items()]

            return client.put_object_tagging(
                Bucket=cls._s3_get_bucket(),
                Key=key,
                Tagging={"TagSet": new_tags},  # type: ignore
            )

    # ....................... #

    @classmethod
    def s3_remove_file_tags(cls, key: str, tags: List[str]):
        """
        Remove file tags

        Args:
            key (str): File key
            tags (list): File tags
        """

        with cls.__s3_client() as client:  # type: ignore
            existing_tags = cls.s3_get_file_tags(key)
            merged_tags = {k: v for k, v in existing_tags.items() if k not in tags}
            new_tags = [{"Key": k, "Value": v} for k, v in merged_tags.items()]

            return client.put_object_tagging(
                Bucket=cls._s3_get_bucket(),
                Key=key,
                Tagging={"TagSet": new_tags},  # type: ignore
            )

    # ....................... #

    @classmethod
    def s3_list_files(cls, blob: str, page: int = 1, size: int = 20):
        """
        List bucket files

        Args:
            blob (str): Blob name
            page (int): Page number
            size (int): Page size

        Returns:
            response (TableResponse): Response
        """

        with cls.__s3_client() as client:  # type: ignore
            paginator = client.get_paginator("list_objects_v2")
            iterator = paginator.paginate(
                Bucket=cls._s3_get_bucket(),
                Prefix=blob,
                FetchOwner=False,
                PaginationConfig={"MaxItems": size},
            )

            for i, r in enumerate(iterator):
                if i + 1 == page:
                    res = r
                    break

            contents = res.pop("Contents", [])
            hits = []

            for x in contents:
                tags = cls.s3_get_file_tags(x["Key"])  # type: ignore
                hits.append(S3File.from_s3_object(x, tags))  # type: ignore

            return TableResponse(
                hits=TabularData(hits),
                size=size,
                page=page,
                count=res["KeyCount"],
            )

    # ....................... #

    @classmethod
    def s3_upload_file(
        cls,
        key: str,
        file: bytes,
        avoid_duplicates: bool = False,
    ) -> str:
        """
        Upload a file

        Args:
            key (str): File key
            file (bytes): File content
            avoid_duplicates (bool): Whether to avoid duplicates

        Returns:
            key (str): File key
        """

        with cls.__s3_client() as client:  # type: ignore
            if cls.s3_file_exists(key):
                if avoid_duplicates:
                    key_ = key.split(".")
                    ext = key_[-1]
                    key_join = ".".join(key_[:-1])

                    pattern = re.compile(r"\(\d+\)$")
                    match = pattern.search(key_join)
                    new_key = key_join

                    if match:
                        n = int(match.group()[1:-1])
                        new_key = key_join[: match.start()] + f"({n + 1})"

                    else:
                        n = 1
                        new_key = f"{new_key}(1)"

                    while cls.s3_file_exists(new_key + f".{ext}"):
                        n += 1
                        new_key = "(".join(new_key.split("(")[:-1]) + f"({n + 1})"

                    key = f"{new_key}.{ext}"

                else:
                    raise Conflict("File already exists.")

            client.upload_fileobj(
                Fileobj=file,  # type: ignore
                Bucket=cls._s3_get_bucket(),
                Key=key,
            )

            return key

    # ....................... #

    @classmethod
    def s3_download_file(cls, key: str):
        """
        Download a file

        Args:
            key (str): File key

        Returns:
            file (bytes): File content
        """

        with cls.__s3_client() as client:  # type: ignore
            return client.get_object(
                Bucket=cls._s3_get_bucket(),
                Key=key,
            )

    # ....................... #

    @classmethod
    def s3_delete_file(cls, key: str):
        """
        Delete a file

        Args:
            key (str): File key
        """

        with cls.__s3_client() as client:  # type: ignore
            return client.delete_object(
                Bucket=cls._s3_get_bucket(),
                Key=key,
            )
