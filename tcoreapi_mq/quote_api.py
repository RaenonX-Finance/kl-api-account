import json
import re

from .core import TCoreZMQ


class QuoteAPI(TCoreZMQ):
    def __init__(self, app_id, service_key):
        super().__init__(app_id, service_key)

    def subscribe_realtime(self, symbol):
        with self.lock:
            obj = {
                "Request": "SUBQUOTE", "SessionKey": self.session_key,
                "Param": {
                    "Symbol": symbol,
                    "SubDataType": "REALTIME"
                }
            }
            self.socket.send_string(json.dumps(obj))
            message = self.socket.get_message()
            data = json.loads(message)

        return data

    def unsubscribe_realtime(self, sessionKey, symbol):
        self.lock.acquire()
        obj = {"Request": "UNSUBQUOTE", "SessionKey": sessionKey}
        obj["Param"] = {"Symbol": symbol, "SubDataType": "REALTIME"}
        self.socket.send_string(json.dumps(obj))
        message = self.socket.get_message()
        data = json.loads(message)
        self.lock.release()
        return data

    def SubGreeks(self, sessionKey, symbol, greeksType="REAL"):
        self.lock.acquire()
        obj = {"Request": "SUBQUOTE", "SessionKey": sessionKey}
        obj["Param"] = {"Symbol": symbol, "SubDataType": "GREEKS", "GreeksType": greeksType}
        self.socket.send_string(json.dumps(obj))
        message = self.socket.get_message()
        data = json.loads(message)
        self.lock.release()
        return data

    def UnsubGreeks(self, sessionKey, symbol, greeksType="REAL"):
        self.lock.acquire()
        obj = {"Request": "UNSUBQUOTE", "SessionKey": sessionKey}
        obj["Param"] = {"Symbol": symbol, "SubDataType": "GREEKS", "GreeksType": greeksType}
        self.socket.send_string(json.dumps(obj))
        message = self.socket.get_message()
        data = json.loads(message)
        self.lock.release()
        return data

    # 订阅历史数据
    # 1：SessionKey，
    # 2：合约代码，
    # 3：数据周期:"TICKS","1K","DK"，
    # 4: 历史数据开始时间,
    # 5: 历史数据结束时间
    def subscribe_history(self, sessionKey, symbol, type, startTime, endTime):
        self.lock.acquire()
        obj = {"Request": "SUBQUOTE", "SessionKey": sessionKey}
        obj["Param"] = {"Symbol": symbol, "SubDataType": type, "StartTime": startTime, "EndTime": endTime}
        self.socket.send_string(json.dumps(obj))
        message = self.socket.get_message()
        data = json.loads(message)
        self.lock.release()
        return data

    # 分页获取订阅的历史数据
    def GetHistory(self, sessionKey, symbol, type, startTime, endTime, qryIndex):
        self.lock.acquire()
        obj = {"Request": "GETHISDATA", "SessionKey": sessionKey}
        obj["Param"] = {
            "Symbol": symbol, "SubDataType": type, "StartTime": startTime, "EndTime": endTime, "QryIndex": qryIndex
        }
        self.socket.send_string(json.dumps(obj))
        message = self.socket.get_message()
        index = re.search(":", message).span()[1]  # filter
        message = message[index:]
        message = json.loads(message)
        self.lock.release()
        return message
