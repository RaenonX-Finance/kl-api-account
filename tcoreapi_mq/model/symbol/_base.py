from abc import ABC


class SymbolBase(ABC):
    @property
    def symbol(self) -> str:
        raise NotImplementedError()

    @property
    def symbol_complete(self) -> str:
        raise NotImplementedError()
