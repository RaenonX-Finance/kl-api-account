from abc import ABC


class SymbolBase(ABC):
    @property
    def symbol_name(self) -> str:
        raise NotImplementedError()
