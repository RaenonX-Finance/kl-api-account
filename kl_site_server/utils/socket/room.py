TYPE_PARAM_SEP: str = "|"

TYPE_NAME_PX_SUB: str = "Px"

PX_SUB_SECURITY_SEP: str = "/"


def make_px_sub_room_name(securities: list[str]) -> str:
    return f"{TYPE_NAME_PX_SUB}{TYPE_PARAM_SEP}{PX_SUB_SECURITY_SEP.join(securities)}"


def get_px_sub_securities_from_room_name(name: str | None) -> list[str]:
    if not name or TYPE_PARAM_SEP not in name:
        # Room name could be `None`, or any other untyped name
        return []

    type_, params = name.split(TYPE_PARAM_SEP, 1)

    if type_ != TYPE_NAME_PX_SUB:
        return []

    return params.split(PX_SUB_SECURITY_SEP)
