from datetime import timedelta

from fastapi import Body, Depends
from fastapi.security import OAuth2PasswordRequestForm
from jose import ExpiredSignatureError, JWTError

from kl_site_common.env import FASTAPI_AUTH_CALLBACK, FAST_API_AUTH_TOKEN_EXPIRY_MINS
from ..const import auth_db_users, auth_db_validation, auth_oauth2_scheme
from ..exceptions import generate_bad_request_exception, generate_blocked_exception, generate_unauthorized_exception
from ..model import DbUserModel, RefreshAccessTokenModel, UserDataModel
from ..secret import create_access_token, decode_access_token, is_password_match


def get_user_by_username(
    username: str
) -> DbUserModel | None:
    find_one_result = auth_db_users.find_one({"username": username})

    if not find_one_result:
        return None

    return DbUserModel(**find_one_result)


def get_user_data_by_username(
    username: str
) -> UserDataModel | None:
    find_one_result = auth_db_users.find_one({"username": username})

    if not find_one_result:
        return None

    # Remove secrets from the returned model
    # > This is exactly the prop difference between `DbUserModel` and `UserDataModel`
    find_one_result.pop("hashed_password")

    return UserDataModel(**find_one_result)


def get_user_data_by_oauth2_token(
    token: str = Depends(auth_oauth2_scheme)
) -> UserDataModel:
    try:
        payload = decode_access_token(token)
        username: str = payload.get("sub")

        if username is None:
            raise generate_unauthorized_exception("Invalid token - no user name")
    except ExpiredSignatureError:
        raise generate_unauthorized_exception("Invalid token - signature expired")
    except JWTError as ex:
        raise generate_unauthorized_exception(f"Invalid token - JWT decode error: {type(ex)}")

    user = get_user_data_by_username(username)
    if user is None:
        raise generate_unauthorized_exception("Invalid token - user not exists")

    return user


def get_active_user_by_oauth2_token(
    current_user: UserDataModel = Depends(get_user_data_by_oauth2_token)
) -> UserDataModel:
    if current_user.blocked:
        raise generate_blocked_exception()

    return current_user


def get_admin_user_by_oauth2_token(
    current_user: UserDataModel = Depends(get_active_user_by_oauth2_token)
) -> UserDataModel:
    if not current_user.admin:
        raise generate_unauthorized_exception("Insufficient permission")

    return current_user


def authenticate_user_by_credentials(
    form: OAuth2PasswordRequestForm = Depends()
) -> DbUserModel:
    user = get_user_by_username(form.username)

    if not user:
        raise generate_unauthorized_exception("User not exists")

    if not is_password_match(form.password, user.hashed_password):
        raise generate_unauthorized_exception("Incorrect password")

    return user


def generate_access_token_on_doc(
    user: DbUserModel = Depends(authenticate_user_by_credentials)
) -> str:
    return create_access_token(
        username=user.username,
        expiry_delta=timedelta(minutes=FAST_API_AUTH_TOKEN_EXPIRY_MINS)
    )


def authenticate_user_with_callback(
    form: OAuth2PasswordRequestForm = Depends(),
    redirect_uri: str = Body(...),
) -> DbUserModel:
    if not auth_db_validation.find_one({"client_id": form.client_id}):
        raise generate_bad_request_exception("Invalid client ID")

    if FASTAPI_AUTH_CALLBACK != redirect_uri:
        raise generate_bad_request_exception("Callback URI mismatch")

    return authenticate_user_by_credentials(form)


def generate_access_token(
    user: DbUserModel = Depends(authenticate_user_with_callback)
) -> str:
    return create_access_token(
        username=user.username,
        expiry_delta=timedelta(minutes=FAST_API_AUTH_TOKEN_EXPIRY_MINS)
    )


def refresh_access_token(
    body: RefreshAccessTokenModel = Depends(),
    user_data: UserDataModel = Depends(get_user_data_by_oauth2_token)
) -> str:
    if not auth_db_validation.find_one({"client_id": body.client_id, "client_secret": body.client_secret}):
        raise generate_bad_request_exception("Invalid client ID or secret")

    return create_access_token(
        username=user_data.username,
        expiry_delta=timedelta(minutes=FAST_API_AUTH_TOKEN_EXPIRY_MINS)
    )
