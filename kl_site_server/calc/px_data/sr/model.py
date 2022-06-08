from dataclasses import dataclass, field

from kl_site_common.utils import avg


@dataclass(kw_only=True)
class SRLevelGroup:
    levels: list[float] = field(default_factory=list)

    @staticmethod
    def from_levels_to_groups(levels: list[float], min_gap: float) -> list["SRLevelGroup"]:
        current_group_start_level = None
        current_group = SRLevelGroup()
        groups = []

        for level in sorted(levels):
            if not current_group_start_level:
                current_group_start_level = level
            elif level - current_group_start_level >= min_gap:
                current_group_start_level = level
                groups.append(current_group)
                current_group = SRLevelGroup()

            current_group.levels.append(level)

        groups.append(current_group)

        return groups

    @property
    def mean(self) -> float:
        return avg(self.levels)


@dataclass(kw_only=True)
class SRLevel:
    level: float
    strength: float

    @staticmethod
    def from_level_group(group: SRLevelGroup) -> "SRLevel":
        return SRLevel(
            level=group.mean,
            strength=len(group.levels)
        )


@dataclass(kw_only=True)
class SRLevelsData:
    levels: list[float]

    min_gap: float

    levels_data: list[SRLevel] = field(init=False)

    def _init_get_level_data(self, groups: list[SRLevelGroup]):
        for group in groups:
            self.levels_data.append(SRLevel.from_level_group(group))

    def __post_init__(self):
        self.levels_data = []
        self._init_get_level_data(SRLevelGroup.from_levels_to_groups(self.levels, self.min_gap))
