# auth_utils.py
import os
from jose import jwt, JWTError, ExpiredSignatureError
import datetime

SECRET = os.getenv("JWT_SECRET", "dev_only_secret_change_me")
ISSUER = os.getenv("JWT_ISSUER", "inv-saas-local")
AUD = os.getenv("JWT_AUDIENCE", "inv-saas-clients")
ALG = "HS256"

class AuthError(Exception):
    def __init__(self, message, status=401):
        super().__init__(message)
        self.status = status

def issue_token(user: dict, ttl_sec: int = 3600) -> str:
    now = datetime.datetime.utcnow()

    payload = {
        "sub": user["userId"],
        "email": user["email"],
        "tenantId": user["tenantId"],  # single tenant per user
        "roles": user["roles"],
        "iat": now,
        "nbf": now,
        "exp": now + datetime.timedelta(seconds=ttl_sec),
        "iss": ISSUER,
        "aud": AUD
    }

    token = jwt.encode(payload, SECRET, algorithm=ALG)
    return token

def decode_token(token: str) -> dict:
    try:
        return jwt.decode(
            token,
            SECRET,
            algorithms=[ALG],
            issuer=ISSUER,
            audience=AUD
        )
    except ExpiredSignatureError:
        raise AuthError("token_expired", 401)
    except JWTError as e:
        raise AuthError(f"invalid_token: {str(e)}", 401)

def get_bearer_token(auth_header: str | None) -> str | None:
    if not auth_header:
        return None
    parts = auth_header.split(" ")
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return None
