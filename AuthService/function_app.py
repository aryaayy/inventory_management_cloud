import azure.functions as func
import datetime
import json
import logging
import uuid
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.auth import issue_token, decode_token, get_bearer_token, AuthError, require_user, require_role

app = func.FunctionApp()

# ---------------- AUTH ----------------
@app.route(route="auth/login", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def login(req: func.HttpRequest) -> func.HttpResponse:
    body = req.get_json()
    email = (body.get("email") or "").lower()

    # MOCK user = 1 tenant only
    user = {
        "userId": f"u-{uuid.uuid4()}",
        "email": email,
        "tenantId": "T001",
        "roles": ["Owner"]  # default Owner
    }

    token = issue_token(user, 3600)
    return func.HttpResponse(json.dumps({"token": token}), mimetype="application/json")


# ---------------- PROTECTED PAGES ----------------

# GET profile
@app.route(route="auth/me", auth_level=func.AuthLevel.FUNCTION)
def me(req: func.HttpRequest) -> func.HttpResponse:
    claims, err = require_user(req)
    if err: return err
    return func.HttpResponse(json.dumps(claims), mimetype="application/json")


# Tenant info
@app.route(route="tenant/info", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def tenant_info(req: func.HttpRequest) -> func.HttpResponse:
    # ambil token dari header
    token = get_bearer_token(req.headers.get("Authorization"))

    # fallback: token via query param untuk demo di browser
    if not token:
        demo_token = req.params.get("token")
        if not demo_token:
            return func.HttpResponse(
                json.dumps({
                    "info": "Gunakan aplikasi / curl untuk akses secure endpoint",
                    "hint": "Tambahkan ?token=JWT_DISINI untuk demo",
                    "example": "/api/tenant/info?token=PASTE_HERE"
                }),
                status_code=200,
                mimetype="application/json"
            )
        token = demo_token
    
    # verify token
    try:
        claims = decode_token(token)
    except Exception as e:
        return func.HttpResponse(f"Invalid token: {e}", status_code=401)

    result = {
        "tenantId": claims.get("tenantId"),
        "name": "Toko Demo " + claims.get("tenantId"),
        "plan": "Premium",
    }

    return func.HttpResponse(json.dumps(result), status_code=200, mimetype="application/json")