from datetime import datetime, timezone
from typing import TYPE_CHECKING

from kl_site_common.db import PyObjectId
from kl_site_common.utils import print_log
from .const import user_db_session
from .model import UserSessionModel

if TYPE_CHECKING:
    from kl_site_server.socket import SocketNamespace


def record_session_connected(
    account_id: PyObjectId,
    namespace: "SocketNamespace",
    session_id: str,
) -> dict["SocketNamespace", str] | None:
    """
    Record the session of ``account_id``.

    Returns the session ID to disconnect where key is the socket namespace; value is the session ID.
    Returns ``None`` if no session disconnection needed.
    """
    session = user_db_session.find_one({"account_id": account_id})

    if not session:
        # No existing session for the account
        model = UserSessionModel(
            account_id=account_id,
            session_id={namespace: session_id},
            last_check=datetime.utcnow().replace(tzinfo=timezone.utc)
        )
        user_db_session.insert_one(model.dict())

        print_log(f"Session [cyan]created[/] for account [yellow]{account_id}[/]")
        return None

    session_model = UserSessionModel(**session)

    if session_model.session_id.get(namespace, session_id) != session_id:
        # Session conflict
        user_db_session.update_one(
            {"account_id": account_id},
            {"$set": {"session_id": {namespace: session_id}}},
        )
        print_log(
            f"Session [bold red]replaced[/] for account [yellow]{account_id}[/] - "
            f"Session ID `[cyan]{session_model.session_id}[/]` to disconnect"
        )
        return session_model.session_id

    user_db_session.update_one(
        {"account_id": account_id},
        {"$set": {f"session_id.{namespace}": session_id}},
    )
    print_log(f"Session [cyan]recorded[/] for account [yellow]{account_id}[/]")
    return None


def record_session_checked(account_id: PyObjectId):
    user_db_session.update_one(
        {"account_id": account_id},
        {"$set": {"last_check": datetime.utcnow().replace(tzinfo=timezone.utc)}}
    )


def record_session_disconnected(
    namespace: "SocketNamespace",
    session_id: str,
):
    filter_ = {f"session_id.{namespace}": session_id}
    user_db_session.find_one_and_update(filter_, {"$unset": filter_})
    print_log(f"Session [yellow]{session_id}[/] in [yellow]{namespace}[/] disconnected")
