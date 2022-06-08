from dataclasses import dataclass


@dataclass(kw_only=True)
class OnErrorEvent:
    message: str

    def __str__(self):
        return f"Error: {self.message}"
