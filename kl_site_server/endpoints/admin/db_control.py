from typing import Any, Callable

from bson import ObjectId
from fastapi import Body, Depends
from pymongo import ReturnDocument

from kl_site_common.db import PyObjectId
from kl_site_server.db import (
    FuturesMarketClosedSessionModel, Permission, UserDataModel, auth_db_users,
    create_new_market_close_session, delete_market_close_session, get_all_market_close_session, user_db_session,
)
from kl_site_server.utils import generate_bad_request_exception, generate_insufficient_permission_exception
from .model import AccountData, BlockedUpdateModel, ExpiryUpdateModel, PermissionUpdateModel, SessionDeleteModel
from ..auth import get_active_user_by_user_data


def require_permission(executor_uid: PyObjectId | None, permission_required: Permission):
    if not executor_uid:
        raise generate_bad_request_exception("Invalid executor UID (None)")

    executor = auth_db_users.find_one({"_id": executor_uid})

    if not executor or not UserDataModel(**executor).has_permission(permission_required):
        raise generate_insufficient_permission_exception([permission_required])


def user_data_dict_to_account_data(
    user_data_raw: dict[str, Any], *,
    online: Callable[[UserDataModel], bool]
) -> AccountData:
    data = UserDataModel(**user_data_raw)

    return AccountData(
        id=str(data.id),
        username=data.username,
        permissions=data.permissions,
        expiry=data.expiry,
        blocked=data.blocked,
        admin=data.admin,
        online=online(data),
    )


def get_account_list(executor: UserDataModel = Depends(get_active_user_by_user_data)) -> list[AccountData]:
    require_permission(executor.id, "account:view")

    logged_in_account_ids = {session["account_id"] for session in user_db_session.find()}

    ret: list[AccountData] = []
    for data in auth_db_users.find({"_id": {"$ne": executor.id}}):
        ret.append(user_data_dict_to_account_data(
            data,
            online=lambda user_data: user_data.id in logged_in_account_ids
        ))

    return ret


def update_account_property(
    executor: UserDataModel,
    required_permission: Permission,
    target_id: ObjectId,
    update: dict[str, Any],
) -> AccountData:
    require_permission(executor.id, required_permission)

    updated_account = auth_db_users.find_one_and_update(
        {"_id": target_id},
        update,
        return_document=ReturnDocument.AFTER
    )

    if not updated_account:
        raise generate_bad_request_exception(f"No matching account to update ({target_id})")

    return user_data_dict_to_account_data(
        updated_account,
        online=lambda user_data: user_db_session.find_one({"account_id": user_data.id}) is not None,
    )


def update_account_expiry(
    executor: UserDataModel = Depends(get_active_user_by_user_data),
    expiry_update_data: ExpiryUpdateModel = Body(...),
) -> AccountData:
    return update_account_property(
        executor,
        "account:expiry",
        ObjectId(expiry_update_data.id),
        {"$set": {"expiry": expiry_update_data.expiry}}
    )


def update_account_blocked(
    executor: UserDataModel = Depends(get_active_user_by_user_data),
    blocked_update_data: BlockedUpdateModel = Body(...),
) -> AccountData:
    return update_account_property(
        executor,
        "account:block",
        ObjectId(blocked_update_data.id),
        {"$set": {"blocked": blocked_update_data.blocked}}
    )


def update_account_permission(
    executor: UserDataModel = Depends(get_active_user_by_user_data),
    permission_update_data: PermissionUpdateModel = Body(...),
) -> AccountData:
    updated_account_data: AccountData | None = None

    # Error if `$addToSet` and `$pullAll` happens in one update operator
    # Therefore splitting operations into 2

    if permission_update_data.add:
        updated_account_data = update_account_property(
            executor,
            "permission:add",
            ObjectId(permission_update_data.id),
            {"$addToSet": {"permissions": {"$each": permission_update_data.add}}}
        )

    if permission_update_data.remove:
        updated_account_data = update_account_property(
            executor,
            "permission:remove",
            ObjectId(permission_update_data.id),
            {"$pullAll": {"permissions": permission_update_data.remove}}
        )

    if updated_account_data is None:
        raise generate_bad_request_exception("Update data should not have both empty `add` and `remove`")

    return updated_account_data


def create_single_market_closed_session(
    executor: UserDataModel = Depends(get_active_user_by_user_data),
    new_session: FuturesMarketClosedSessionModel = Body(...),
) -> list[FuturesMarketClosedSessionModel]:
    require_permission(executor.id, "config:session")

    create_new_market_close_session(new_session)

    return get_all_market_close_session()


def delete_single_market_closed_session(
    executor: UserDataModel = Depends(get_active_user_by_user_data),
    session_to_delete: SessionDeleteModel = Body(...),
) -> list[FuturesMarketClosedSessionModel]:
    require_permission(executor.id, "config:session")

    delete_market_close_session(ObjectId(session_to_delete.session))

    return get_all_market_close_session()


def get_market_closed_session_list(
    executor: UserDataModel = Depends(get_active_user_by_user_data),
) -> list[FuturesMarketClosedSessionModel]:
    require_permission(executor.id, "config:session")

    return get_all_market_close_session()
