import os

from kl_site_common.const import print_configs
from kl_site_common.utils import set_current_process_to_highest_priority
from kl_site_server.app import start_server_app
from kl_site_server.const import fast_api


@fast_api.on_event("startup")
async def startup_event():
    print_configs()
    start_server_app()
    set_current_process_to_highest_priority()


if __name__ == "__main__":
    os.system("uvicorn main:fast_api --port=8000")
