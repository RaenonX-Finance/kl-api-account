from dataclasses import dataclass


@dataclass(kw_only=True)
class SRLevelsData:
    groups: list[list[float]]
