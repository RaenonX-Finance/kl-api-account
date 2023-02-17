from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class PxCalcReply(_message.Message):
    __slots__ = ["message"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    message: str
    def __init__(self, message: _Optional[str] = ...) -> None: ...

class PxCalcRequestMulti(_message.Message):
    __slots__ = ["symbols"]
    SYMBOLS_FIELD_NUMBER: _ClassVar[int]
    symbols: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, symbols: _Optional[_Iterable[str]] = ...) -> None: ...

class PxCalcRequestSingle(_message.Message):
    __slots__ = ["symbol"]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    symbol: str
    def __init__(self, symbol: _Optional[str] = ...) -> None: ...
