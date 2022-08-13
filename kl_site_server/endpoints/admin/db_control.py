from typing import Any

from bson import ObjectId
from fastapi import Body, Depends
from pymongo import ReturnDocument

from kl_site_common.db import PyObjectId
from kl_site_server.db import Permission, UserDataModel, auth_db_users, user_db_session
from kl_site_server.utils import generate_bad_request_exception, generate_insufficient_permission_exception
from .model import AccountData, BlockedUpdateModel, ExpiryUpdateModel
from ..auth import get_active_user_by_user_data


def permission_check(executor_uid: PyObjectId | None, permission_required: Permission):
    if not executor_uid:
        raise generate_bad_request_exception("Invalid executor UID (None)")

    executor = auth_db_users.find_one({"_id": executor_uid})

    if not executor or not UserDataModel(**executor).has_permission(permission_required):
        raise generate_insufficient_permission_exception([permission_required])


def user_data_dict_to_account_data(user_data_raw: dict[str, Any], *, online: bool) -> AccountData:
    data = UserDataModel(**user_data_raw)

    return AccountData(
        id=str(data.id),
        username=data.username,
        permissions=data.permissions,
        expiry=data.expiry,
        blocked=data.blocked,
        admin=data.admin,
        online=online,
    )


def get_account_list(user: UserDataModel = Depends(get_active_user_by_user_data)) -> list[AccountData]:
    permission_check(user.id, "account:view")

    logged_in_account_ids = {session["account_id"] for session in user_db_session.find()}

    ret: list[AccountData] = []
    for data in auth_db_users.find({"_id": {"$ne": user.id}}):
        ret.append(user_data_dict_to_account_data(data, online=data["_id"] in logged_in_account_ids))

    return ret


def update_account_expiry(
    user: UserDataModel = Depends(get_active_user_by_user_data),
    expiry_update_data: ExpiryUpdateModel = Body(...),
) -> AccountData:
    permission_check(user.id, "account:expiry")

    updated_account = auth_db_users.find_one_and_update(
        {"_id": ObjectId(expiry_update_data.id)},
        {"$set": {"expiry": expiry_update_data.expiry}},
        return_document=ReturnDocument.AFTER,
    )

    if not updated_account:
        raise generate_bad_request_exception(f"No matching account to update ({expiry_update_data.id})")

    return user_data_dict_to_account_data(
        updated_account,
        online=user_db_session.find_one({"account_id": ObjectId(updated_account.id)}) is not None,
    )


def update_account_blocked(
    user: UserDataModel = Depends(get_active_user_by_user_data),
    blocked_update_data: BlockedUpdateModel = Body(...),
) -> AccountData:
    permission_check(user.id, "account:block")

    updated_account = auth_db_users.find_one_and_update(
        {"_id": ObjectId(blocked_update_data.id)},
        {"$set": {"blocked": blocked_update_data.blocked}},
        return_document=ReturnDocument.AFTER,
    )

    if not updated_account:
        raise generate_bad_request_exception(f"No matching account to update ({blocked_update_data.id})")

    return user_data_dict_to_account_data(
        updated_account,
        online=user_db_session.find_one({"account_id": ObjectId(updated_account["_id"])}) is not None,
    )
