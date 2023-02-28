TYPE_PARAM_SEP: str = "|"

TYPE_NAME_PX_SUB: str = "PxSub"
TYPE_NAME_PX_DATA: str = "PxData"

PX_SUB_SECURITY_SEP: str = "/"
PX_DATA_IDENTIFIER_SEP: str = "/"


def get_params_of_type(name: str, type_name: str, params_sep: str) -> list[str]:
    if not name or TYPE_PARAM_SEP not in name:
        # Room name could be `None`, or any other untyped name
        return []

    type_, params = name.split(TYPE_PARAM_SEP, 1)

    if type_ != type_name:
        return []

    return params.split(params_sep)


def make_px_sub_room_name(identifiers: list[str]) -> str:
    securities = set(identifier.split("@", 1)[0] for identifier in identifiers)

    return f"{TYPE_NAME_PX_SUB}{TYPE_PARAM_SEP}{PX_SUB_SECURITY_SEP.join(securities)}"


def get_px_sub_securities_from_room_name(name: str | None) -> list[str]:
    return get_params_of_type(name, TYPE_NAME_PX_SUB, PX_SUB_SECURITY_SEP)


def make_px_data_room_name(identifiers: list[str]) -> str:
    return f"{TYPE_NAME_PX_DATA}{TYPE_PARAM_SEP}{PX_DATA_IDENTIFIER_SEP.join(identifiers)}"


def get_px_data_identifiers_from_room_name(name: str | None) -> list[str]:
    return get_params_of_type(name, TYPE_NAME_PX_DATA, PX_DATA_IDENTIFIER_SEP)
