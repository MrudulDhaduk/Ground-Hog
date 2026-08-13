"""Shared test scaffolding."""

from collections.abc import Mapping

from groundhog.types import JsonValue


class ListTrace:
    """An in-memory `Trace`.

    Satisfies the Protocol structurally -- no base class, no registration. Which is
    itself worth knowing: if this stopped type-checking, the `Trace` Protocol would have
    grown a requirement that a real implementation should not have needed.
    """

    def __init__(self) -> None:
        self.records: list[Mapping[str, JsonValue]] = []

    def write(self, record: Mapping[str, JsonValue]) -> None:
        self.records.append(record)

    def kinds(self) -> list[str]:
        return [str(record["kind"]) for record in self.records]

    def of_kind(self, kind: str) -> list[Mapping[str, JsonValue]]:
        return [record for record in self.records if record["kind"] == kind]
