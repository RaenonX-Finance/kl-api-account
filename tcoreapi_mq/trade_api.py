import json

from .core import TCoreZMQ


class TradeAPI(TCoreZMQ):
    def __init__(self, app_id, service_key):
        super().__init__(app_id, service_key)

    # 已登入资金账户
    def QryAccount(self, sessionKey):
        self.lock.acquire()
        obj = {"Request": "ACCOUNTS", "SessionKey": sessionKey}
        self.socket.send_string(json.dumps(obj))
        message = self.socket.recv()[:-1]
        data = json.loads(message)
        self.lock.release()
        return data

    # 查询当日委托回报
    def QryReport(self, sessionKey, qryIndex):
        self.lock.acquire()
        obj = {"Request": "RESTOREREPORT", "SessionKey": sessionKey, "QryIndex": qryIndex}
        self.socket.send_string(json.dumps(obj))
        message = self.socket.recv()[:-1]
        data = json.loads(message)
        self.lock.release()
        return data

    # 查询当日成交回报
    def QryFillReport(self, sessionKey, qryIndex):
        self.lock.acquire()
        obj = {"Request": "RESTOREFILLREPORT", "SessionKey": sessionKey, "QryIndex": qryIndex}
        self.socket.send_string(json.dumps(obj))
        message = self.socket.recv()[:-1]
        data = json.loads(message)
        self.lock.release()
        return data

    # 下单
    def NewOrder(self, sessionKey, param):
        self.lock.acquire()
        obj = {"Request": "NEWORDER", "SessionKey": sessionKey}
        obj["Param"] = param
        self.socket.send_string(json.dumps(obj))
        message = self.socket.recv()[:-1]
        data = json.loads(message)
        self.lock.release()
        return data

    # 改单
    def ReplaceOrder(self, sessionKey, param):
        self.lock.acquire()
        obj = {"Request": "REPLACEORDER", "SessionKey": sessionKey}
        obj["Param"] = param
        self.socket.send_string(json.dumps(obj))
        message = self.socket.recv()[:-1]
        data = json.loads(message)
        self.lock.release()
        return data

    # 删单
    def CancelOrder(self, sessionKey, param):
        self.lock.acquire()
        obj = {"Request": "CANCELORDER", "SessionKey": sessionKey}
        obj["Param"] = param
        self.socket.send_string(json.dumps(obj))
        message = self.socket.recv()[:-1]
        data = json.loads(message)
        self.lock.release()
        return data

    # 查询资金
    def QryMargin(self, sessionKey, accountMask):
        self.lock.acquire()
        obj = {"Request": "MARGINS", "SessionKey": sessionKey, "AccountMask": accountMask}
        self.socket.send_string(json.dumps(obj))
        message = self.socket.recv()[:-1]
        data = json.loads(message)
        self.lock.release()
        return data

    # 查询持仓
    def QryPosition(self, sessionKey, accountMask, qryIndex):
        self.lock.acquire()
        obj = {"Request": "POSITIONS", "SessionKey": sessionKey, "AccountMask": accountMask, "QryIndex": qryIndex}
        self.socket.send_string(json.dumps(obj))
        message = self.socket.recv()[:-1]
        data = json.loads(message)
        self.lock.release()
        return data
