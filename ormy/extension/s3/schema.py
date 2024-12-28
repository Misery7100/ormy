import os
from typing import Dict, Type, TypeVar

from pydantic import Field, computed_field

from ormy.base.pydantic import Base

# ----------------------- #

Sf = TypeVar("Sf", bound="S3File")

# ----------------------- #


class S3File(Base):
    filename: str = Field(
        default=...,
    )
    size_bytes: int = Field(
        default=...,
    )
    path: str = Field(
        default=...,
    )
    last_modified: int = Field(
        default=...,
    )
    tags: Dict[str, str] = Field(
        default_factory=dict,
    )

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
    def from_s3_object(
        cls: Type[Sf],
        obj: dict,
        tags: Dict[str, str] = {},
    ) -> Sf:
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
