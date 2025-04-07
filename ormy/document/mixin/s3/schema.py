import os

from pydantic import computed_field

from ormy.base.pydantic import Base

# ----------------------- #


class S3File(Base):
    """
    S3 file schema

    Attributes:
        filename (str): The filename of the file.
        size_bytes (int): The size of the file in bytes.
        path (str): The path of the file.
        last_modified (int): The last modified timestamp of the file.
        tags (dict[str, str]): The tags of the file.
        size_kb (float, computed): The size of the file in kilobytes.
        size_mb (float, computed): The size of the file in megabytes.
        file_type (str, computed): The type of the file.
    """

    filename: str
    size_bytes: int
    path: str
    last_modified: int
    tags: dict[str, str] = {}

    # ....................... #

    @computed_field  # type: ignore[misc]
    @property
    def size_kb(self) -> float:
        return round(self.size_bytes / 1024, 2)

    @computed_field  # type: ignore[misc]
    @property
    def size_mb(self) -> float:
        return round(self.size_kb / 1024, 2)

    @computed_field  # type: ignore[misc]
    @property
    def file_type(self) -> str:
        return self.filename.split(".")[-1]

    # ....................... #

    @classmethod
    def from_s3_object(cls, obj: dict, tags: dict[str, str] = {}):
        """
        Create a new S3File instance from an S3 object

        Args:
            obj (dict): The S3 object.
            tags (dict[str, str]): The tags of the file.

        Returns:
            result (S3File): The new S3File instance.
        """

        path = obj["Key"]
        filename = os.path.basename(path)
        size = int(obj["Size"])

        return cls(
            filename=filename,
            size_bytes=size,
            path=path,
            last_modified=int(obj["LastModified"].timestamp()),
            tags=tags,
        )
