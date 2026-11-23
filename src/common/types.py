from dataclasses import dataclass
from enum import Enum


class Outcome(str, Enum):
    INSERTED = "inserted"
    UPDATED = "updated"
    UNCHANGED = "unchanged"
    DELETED = "deleted"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class HandleResult:
    outcome: Outcome
    detail: str = ""
