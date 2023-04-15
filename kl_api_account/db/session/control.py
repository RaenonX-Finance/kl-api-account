from datetime import datetime, timezone

from kl_api_common.db import PyObjectId
from kl_api_common.utils import print_log, print_socket_event
from .const import user_db_session
from .model import UserSessionModel


def record_session_connected(
    account_id: PyObjectId,
    session_id: str,
) -> str | None:
    """
    Record the session of ``account_id``.

    Returns the session ID to disconnect; ``None`` if no session disconnection needed.
    """
    session = user_db_session.find_one({"account_id": account_id})

    if not session:
        # No existing session for the account
        model = UserSessionModel(
            account_id=account_id,
            session_id=session_id,
            last_check=datetime.utcnow().replace(tzinfo=timezone.utc)
        )
        user_db_session.insert_one(model.dict())

        print_log(
            f"Session [cyan]created[/] for account [yellow]{account_id}[/] - SID: `[cyan]{session_id}[/]`",
            accountId=account_id, sessionId=session_id
        )
        return None

    session_model = UserSessionModel(**session)

    if session_model.session_id != session_id:
        # Session conflict
        user_db_session.update_one(
            {"account_id": account_id},
            {"$set": {"session_id": session_id}},
        )
        print_log(
            f"Session [bold red]replaced[/] for account [yellow]{account_id}[/] - "
            f"SID: `[cyan]{session_model.session_id}[/]` -> `[cyan]{session_id}[/]`",
            accountId=account_id, sessionId={"old": session_model.session_id, "new": session_id}
        )
        return session_model.session_id

    user_db_session.update_one(
        {"account_id": account_id},
        {"$set": {"session_id": session_id}},
    )
    print_log(f"Session [cyan]recorded[/] for account [yellow]{account_id}[/]", accountId=account_id)
    return None


def record_session_checked(account_id: PyObjectId):
    user_db_session.update_one(
        {"account_id": account_id},
        {"$set": {"last_check": datetime.utcnow().replace(tzinfo=timezone.utc)}}
    )


def record_session_disconnected(session_id: str):
    user_db_session.delete_one({"session_id": session_id})
    print_socket_event("disconnect", session_id=session_id)
