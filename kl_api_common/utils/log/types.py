from typing import Literal, Optional, TypeAlias, TypeGuard, TypedDict

LogLevels: TypeAlias = Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]


class LogDataRequired(TypedDict):
    level: LogLevels
    threadId: int
    timestamp: str
    message: str
    identifier: Optional[str]


LogData: TypeAlias = TypeGuard[LogDataRequired]
