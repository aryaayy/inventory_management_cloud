import azure.functions as func
import datetime
import json
import logging
import requests
import uuid
from auth_utils import issue_token, decode_token, get_bearer_token, AuthError


app = func.FunctionApp()

@app.route(route="MyHttpTrigger", auth_level=func.AuthLevel.FUNCTION)
def MyHttpTrigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
    
@app.route(route="GetProducts", auth_level=func.AuthLevel.FUNCTION)
def GetProducts(req: func.HttpRequest) -> func.HttpResponse:
    response = requests.get("https://api.mockfly.dev/mocks/85f777c7-5caf-41a2-9c31-3ce762be6265/api/products")
    print(response.json()[0])

    return func.HttpResponse(
        json.dumps(response.json(), indent=4),
        status_code=200
    )
    
# ---------------- PUBLIC PAGES ----------------
@app.route(route="products", auth_level=func.AuthLevel.ANONYMOUS)
def get_products(req: func.HttpRequest) -> func.HttpResponse:
    data = [
        {"id": "P001", "name": "Kaos Polos", "stock": 10},
        {"id": "P002", "name": "Hoodie", "stock": 5},
        {"id": "P003", "name": "Topi", "stock": 12},
    ]
    return func.HttpResponse(json.dumps(data), mimetype="application/json")


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


# Util – response helper
def error(msg, code):
    return func.HttpResponse(json.dumps({"error": msg}), mimetype="application/json", status_code=code)

# Middleware-like helper
def require_user(req):
    token = get_bearer_token(req.headers.get("Authorization"))
    if not token:
        return None, error("token_required", 401)
    try:
        return decode_token(token), None
    except Exception as e:
        return None, error("invalid_token", 401)

def require_role(claims, allowed_roles):
    roles = claims.get("roles", [])
    return any(r in allowed_roles for r in roles)


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



# Product management (Owner only)
@app.route(route="products/manage", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def manage_products(req: func.HttpRequest) -> func.HttpResponse:
    claims, err = require_user(req)
    if err: return err

    if not require_role(claims, ["Owner"]):
        return error("forbidden: owner only", 403)

    return func.HttpResponse(
        json.dumps({"admin_products": ["Tambah produk", "Edit", "Delete"]}, indent=2),
        mimetype="application/json"
    )