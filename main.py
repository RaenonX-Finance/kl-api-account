import os

from kl_site_common.const import print_configs
from kl_site_common.utils import set_current_process_to_highest_priority
from kl_site_grpc.client import grpc_channel
from kl_site_server.app import start_server_app
from kl_site_server.const import fast_api


@fast_api.on_event("startup")
async def startup_event():
    print_configs()
    start_server_app()
    set_current_process_to_highest_priority()


@fast_api.on_event("shutdown")
async def shutdown_event():
    grpc_channel.close()


if __name__ == "__main__":
    # Using this instead of `uvicorn` API to avoid starting the main client.py twice
    # https://stackoverflow.com/a/66197795/11571888
    os.system("uvicorn main:fast_api --port=8000")
