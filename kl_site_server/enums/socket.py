class GeneralSocketEvent:
    INIT = "init"
    ERROR = "error"
    SIGN_IN = "signIn"
    PING = "ping"
    AUTH = "auth"


class PxSocketEvent:
    PX_INIT = "pxInit"
    UPDATED = "updated"
    REQUEST = "request"
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    MIN_CHANGE = "minChange"
