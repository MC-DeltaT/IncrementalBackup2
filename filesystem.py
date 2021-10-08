from dataclasses import dataclass, field
from datetime import datetime
from typing import List


__all__ = [
    'Directory',
    'File'
]


@dataclass
class File:
    name: str
    last_modified: datetime


@dataclass
class Directory:
    name: str
    files: List[File] = field(default_factory=list)
    subdirectories: List['Directory'] = field(default_factory=list)
