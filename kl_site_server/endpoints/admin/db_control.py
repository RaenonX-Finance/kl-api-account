from bson import ObjectId
from fastapi import Body, Depends

from kl_site_server.db import UserDataModel, auth_db_users, user_db_session
from kl_site_server.utils import generate_bad_request_exception, generate_insufficient_permission_exception
from .model import AccountData, ExpiryUpdateModel
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
            id=str(data.id),
            username=data.username,
            permissions=data.permissions,
            expiry=data.expiry,
            blocked=data.blocked,
            admin=data.admin,
            online=data.id in logged_in_account_ids,
        ))

    return ret


def update_account_expiry(
    user: UserDataModel = Depends(get_active_user_by_user_data),
    expiry_update_data: ExpiryUpdateModel = Body(...),
) -> None:
    executor = auth_db_users.find_one({"_id": user.id})

    if not executor or not UserDataModel(**executor).has_permission("account:expiry"):
        raise generate_insufficient_permission_exception(["account:expiry"])

    update_result = auth_db_users.update_one(
        {"_id": ObjectId(expiry_update_data.id)},
        {"$set": {"expiry": expiry_update_data.expiry}}
    )

    if not update_result.matched_count:
        raise generate_bad_request_exception(f"No matching account to update ({expiry_update_data.id})")
