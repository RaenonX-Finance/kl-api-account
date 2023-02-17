import threading
import time

import grpc

from kl_site_common.const import GRPC_PX_CALC
from kl_site_common.utils import print_log
from kl_site_proto import pxData_pb2, pxData_pb2_grpc

grpc_channel = grpc.insecure_channel(GRPC_PX_CALC)
grpc_px_service = pxData_pb2_grpc.PxDataStub(grpc_channel)


class GrpcClient:
    @staticmethod
    def _calc_last_internal(symbol_complete: str):
        _start = time.time()

        print_log("Send [blue]KLSite.PxData/CalcLast[/blue]", identifier="gRPC")
        grpc_px_service.CalcLast(pxData_pb2.PxCalcRequestSingle(symbol=symbol_complete))
        print_log(
            f"Completed [blue]KLSite.PxData/CalcLast[/blue] in {time.time() - _start:.3f} s",
            identifier="gRPC"
        )

    @staticmethod
    def calc_last_fire(symbol_complete: str):
        threading.Thread(target=GrpcClient._calc_last_internal, args=(symbol_complete,)).start()

    @staticmethod
    def _calc_partial_internal(symbol_complete_list: list[str]):
        _start = time.time()

        print_log("Send [blue]KLSite.PxData/CalcPartial[/blue]", identifier="gRPC")
        grpc_px_service.CalcPartial(pxData_pb2.PxCalcRequestMulti(symbols=symbol_complete_list))
        print_log(
            f"Completed [blue]KLSite.PxData/CalcPartial[/blue] in {time.time() - _start:.3f} s",
            identifier="gRPC"
        )

    @staticmethod
    def calc_partial_fire(symbol_complete_list: list[str]):
        threading.Thread(target=GrpcClient._calc_partial_internal, args=(symbol_complete_list,)).start()

    @staticmethod
    def calc_all(symbol_complete_list: list[str]):
        _start = time.time()

        print_log("Send [blue]KLSite.PxData/CalcAll[/blue]", identifier="gRPC")
        grpc_px_service.CalcAll(pxData_pb2.PxCalcRequestMulti(symbols=symbol_complete_list))
        print_log(f"Completed [blue]KLSite.PxData/CalcAll[/blue] in {time.time() - _start:.3f} s", identifier="gRPC")
