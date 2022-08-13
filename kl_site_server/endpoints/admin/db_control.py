from fastapi import Depends

from kl_site_server.db import UserDataModel, auth_db_users, user_db_session
from kl_site_server.utils import generate_insufficient_permission_exception
from .model import AccountData
from ..auth import get_active_user_by_user_data


def get_account_list(user: UserDataModel = Depends(get_active_user_by_user_data)) -> list[AccountData]:
    executor = auth_db_users.find_one({"_id": user.id})

    if not executor or not UserDataModel(**executor).has_permission("account:view"):
        raise generate_insufficient_permission_exception(["account:view"])

    logged_in_account_ids = {session["account_id"] for session in user_db_session.find()}

    ret: list[AccountData] = []
    for data in auth_db_users.find({"_id": {"$ne": user.id}}):
        data = UserDataModel(**data)

        ret.append(AccountData(
            username=data.username,
            permissions=data.permissions,
            expiry=data.expiry,
            blocked=data.blocked,
            admin=data.admin,
            online=data.id in logged_in_account_ids,
        ))

    return ret
