from base64 import standard_b64decode
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Union, cast
import json

from pendulum import DateTime
from pendulum import parse as parse_time


@dataclass
class Project:
    id: str
    name: str

    @classmethod
    def from_dict(cls, data: dict) -> "Project":
        return cls(**map_keys(data, ["id", "name"]))


@dataclass
class Version:
    id: str
    status: str
    created_at: DateTime
    released_at: DateTime

    @classmethod
    def from_dict(cls, data: dict) -> "Version":
        created_at = cast(DateTime, parse_time(data["createdAt"]))
        released_at = cast(DateTime, parse_time(data["releasedAt"]))
        return cls(id=data["id"], status=data["status"], created_at=created_at, released_at=released_at)


@dataclass
class Entry:
    type: str
    key: str
    version: str
    encoding: str
    raw_value: str

    @classmethod
    def from_dict(cls, data: dict) -> "Entry":
        mapped = map_keys(data, ["type", "key", "version", "encoding", ("value", "raw_value")])
        return cls(**mapped)

    @property
    def value(self) -> Any:
        if self.type == "secret" and self.raw_value == "<secret-concealed>":
            raise ErrorSecretConcealed(f"secret value concealed: {self.key}")

        if self.encoding == "raw":
            return self.raw_value
        elif self.encoding == "json":
            return json.loads(self.raw_value)
        elif self.encoding == "base64":
            return standard_b64decode(self.raw_value)
        else:
            raise ValueError("unknown encoding: %s" % self.encoding)


class ErrorSecretConcealed(Exception):
    pass


@dataclass
class VersionRelease:

    version: Optional[Version]
    updated: List[Entry]
    deleted: List[str]

    @classmethod
    def from_dict(cls, data: dict) -> "VersionRelease":
        updated = [Entry.from_dict(item) for item in data["updatedEntries"]]
        version = None

        if data["version"]:
            version = Version.from_dict(data["version"])

        return cls(version=version, updated=updated, deleted=data["deletedEntries"])


def map_keys(data: dict, keys: List[Union[str, Tuple[str, str]]]) -> dict:
    """
    a helper to map API response fields to model fields
    """
    rv = {}

    for key in keys:
        if isinstance(key, tuple):
            orig, new = key
            rv[new] = data[orig]
        else:
            rv[key] = data[key]

    return rv
