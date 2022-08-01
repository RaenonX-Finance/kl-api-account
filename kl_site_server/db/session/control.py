from typing import TYPE_CHECKING

from kl_site_common.db import PyObjectId
from kl_site_common.utils import print_log
from .const import user_db_session
from .model import UserSessionModel

if TYPE_CHECKING:
    from kl_site_server.utils import SocketNamespace


def record_session(
    account_id: PyObjectId,
    namespace: "SocketNamespace",
    session_id: str
) -> dict["SocketNamespace", str] | None:
    """
    Record the session of ``account_id``.

    Returns the session ID to disconnect where key is the socket namespace; value is the session ID.
    Returns ``None`` if no session disconnection needed.
    """
    session = user_db_session.find_one({"account_id": account_id})

    if not session:
        # No existing session for the account
        model = UserSessionModel(account_id=account_id, session_id={namespace: session_id})
        user_db_session.insert_one(model.dict())

        print_log(f"[DB-Session] Session [cyan]created[/cyan] for account [yellow]{account_id}[/yellow]")
        return None

    session_model = UserSessionModel(**session)

    if session_model.session_id.get(namespace, session_id) != session_id:
        # Session conflict
        user_db_session.update_one(
            {"account_id": account_id},
            {"$set": {"session_id": {namespace: session_id}}},
        )
        print_log(
            f"[DB-Session] Session [bold][red]replaced[/red][/bold] for account [yellow]{account_id}[/yellow] - "
            f"Session ID `[cyan]{session_model.session_id}[/cyan]` to disconnect"
        )
        return session_model.session_id

    user_db_session.update_one(
        {"account_id": account_id},
        {"$set": {f"session_id.{namespace}": session_id}},
    )
    print_log(f"[DB-Session] Session [cyan]recorded[/cyan] for account [yellow]{account_id}[/yellow]")
    return None


def is_token_active() -> bool:
    pass
