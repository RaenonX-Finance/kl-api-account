from abc import ABC


class SymbolBase(ABC):
    @property
    def security(self) -> str:
        raise NotImplementedError()

    @property
    def symbol_complete(self) -> str:
        raise NotImplementedError()

    def __hash__(self):
        return hash(self.symbol_complete)

    def __eq__(self, other):
        if not isinstance(other, SymbolBase):
            return False

        return hash(self) == hash(other)

    def __str__(self):
        return f"<{self.symbol_complete}>"

    def __repr__(self):
        return str(self)
