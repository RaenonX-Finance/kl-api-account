from fastapi import APIRouter, Depends

from kl_site_server.db import FuturesMarketClosedSessionModel
from .db_control import (
    create_single_market_closed_session, delete_single_market_closed_session, get_account_list,
    get_market_closed_session_list, update_account_blocked,
    update_account_expiry, update_account_permission,
)
from .model import AccountData

admin_router = APIRouter(prefix="/admin")


@admin_router.get(
    "/accounts",
    description="Get a list of accounts.",
    response_model=list[AccountData],
)
async def get_accounts(accounts: list[AccountData] = Depends(get_account_list)) -> list[AccountData]:
    return accounts


@admin_router.post(
    "/update-expiry",
    description="Update the membership expiry of an account.",
    response_model=AccountData,
)
async def update_expiry(updated_account: AccountData = Depends(update_account_expiry)) -> AccountData:
    return updated_account


@admin_router.post(
    "/update-blocked",
    description="Update the blocking status of an account.",
    response_model=AccountData,
)
async def update_blocked(updated_account: AccountData = Depends(update_account_blocked)) -> AccountData:
    return updated_account


@admin_router.post(
    "/update-permissions",
    description="Update permissions of an account.",
    response_model=AccountData,
)
async def update_permissions(updated_account: AccountData = Depends(update_account_permission)) -> AccountData:
    return updated_account


@admin_router.post(
    "/create-closed-session",
    description="Create a market closed session.",
    response_model=list[FuturesMarketClosedSessionModel],
    response_model_by_alias=False,
)
async def create_market_closed_session(
    closed_sessions: list[FuturesMarketClosedSessionModel] = Depends(create_single_market_closed_session)
) -> list[FuturesMarketClosedSessionModel]:
    return closed_sessions


@admin_router.post(
    "/delete-closed-session",
    description="Delete a market closed session.",
    response_model=list[FuturesMarketClosedSessionModel],
    response_model_by_alias=False,
)
async def delete_market_closed_session(
    closed_sessions: list[FuturesMarketClosedSessionModel] = Depends(delete_single_market_closed_session)
) -> list[FuturesMarketClosedSessionModel]:
    return closed_sessions


@admin_router.get(
    "/get-closed-sessions",
    description="Get a list of desginated market closed sessions.",
    response_model=list[FuturesMarketClosedSessionModel],
    response_model_by_alias=False,
)
async def get_market_closed_sessions(
    closed_sessions: list[FuturesMarketClosedSessionModel] = Depends(get_market_closed_session_list)
) -> list[FuturesMarketClosedSessionModel]:
    return closed_sessions
