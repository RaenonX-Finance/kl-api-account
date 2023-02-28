import os

from kl_api_common.const import print_configs
from kl_api_common.utils import set_current_process_to_highest_priority
from kl_api_account.app import start_server_app
from kl_api_account.const import fast_api


@fast_api.on_event("startup")
async def startup_event():
    print_configs()
    start_server_app()
    set_current_process_to_highest_priority()


if __name__ == "__main__":
    # Using this instead of `uvicorn` API to avoid starting the main client.py twice
    # https://stackoverflow.com/a/66197795/11571888
    os.system("uvicorn main:fast_api --port=8000")
