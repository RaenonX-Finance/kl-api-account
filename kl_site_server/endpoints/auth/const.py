from fastapi.security import OAuth2PasswordBearer
from passlib.context import CryptContext

auth_oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token-doc")
auth_crypto_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
