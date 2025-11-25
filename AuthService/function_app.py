import azure.functions as func
import json, uuid, sys, os, requests

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.auth import issue_token, decode_token, get_bearer_token, require_user, require_role, error

app = func.FunctionApp()

PRODUCT_SERVICE_URL = os.getenv("PRODUCT_SERVICE_URL")
INVENTORY_SERVICE_URL = os.getenv("INVENTORY_SERVICE_URL")
REPORT_SERVICE_URL = os.getenv("REPORT_SERVICE_URL")

def proxy(method: str, service_url: str, path: str, token: str | None, body=None) -> func.HttpResponse:
    headers = {"Authorization": token} if token else {}
    resp = requests.request(method, f"{service_url}{path}", headers=headers, json=body)
    return func.HttpResponse(resp.text, status_code=resp.status_code, mimetype="application/json")


# ===== AUTH =====
@app.route(route="auth/login", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def login(req: func.HttpRequest):
    body = req.get_json()
    email = (body.get("email") or "").lower()

    user = {
        "userId": f"u-{uuid.uuid4()}",
        "email": email,
        "tenantId": "T001",
        "roles": ["Owner"]
    }
    token = issue_token(user, 3600)
    return func.HttpResponse(json.dumps({"token": token}), mimetype="application/json")


@app.route(route="auth/me", auth_level=func.AuthLevel.ANONYMOUS)
def me(req: func.HttpRequest):
    claims, resp = require_user(req)
    if resp: return resp
    return func.HttpResponse(json.dumps(claims), mimetype="application/json")


@app.route(route="tenant/info", auth_level=func.AuthLevel.ANONYMOUS)
def tenant_info(req: func.HttpRequest):
    token = get_bearer_token(req.headers.get("Authorization"))
    if not token: return error("token_required", 401)
    claims = decode_token(token)
    return func.HttpResponse(json.dumps({
        "tenantId": claims["tenantId"],
        "storeName": f"Toko Demo {claims['tenantId']}",
        "plan": "Premium"
    }), mimetype="application/json")


# ===== GATEWAY → PRODUCT SERVICE =====
@app.route(route="product/products", auth_level=func.AuthLevel.FUNCTION)
def gw_products(req: func.HttpRequest):
    claims, resp = require_user(req)
    if resp: return resp
    return proxy("GET", PRODUCT_SERVICE_URL, "/product/products", req.headers.get("Authorization"))


@app.route(route="product/manage", auth_level=func.AuthLevel.FUNCTION)
def gw_manage(req: func.HttpRequest):
    claims, resp = require_user(req)
    if resp: return resp
    if not require_role(claims, ["Owner"]): return error("owner_only", 403)
    return proxy("GET", PRODUCT_SERVICE_URL, "/product/manage", req.headers.get("Authorization"))


@app.route(route="product/create", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def gw_create(req: func.HttpRequest):
    claims, resp = require_user(req)
    if resp: return resp
    if not require_role(claims, ["Owner"]): return error("owner_only", 403)
    return proxy("POST", PRODUCT_SERVICE_URL, "/product/create", req.headers.get("Authorization"), req.get_json())


@app.route(route="product/update", methods=["PUT"], auth_level=func.AuthLevel.FUNCTION)
def gw_update(req: func.HttpRequest):
    claims, resp = require_user(req)
    if resp: return resp
    if not require_role(claims, ["Owner"]): return error("owner_only", 403)
    return proxy("PUT", PRODUCT_SERVICE_URL, "/product/update", req.headers.get("Authorization"), req.get_json())


@app.route(route="product/delete", methods=["DELETE"], auth_level=func.AuthLevel.FUNCTION)
def gw_delete(req: func.HttpRequest):
    claims, resp = require_user(req)
    if resp: return resp
    if not require_role(claims, ["Owner"]): return error("owner_only", 403)
    return proxy("DELETE", PRODUCT_SERVICE_URL, "/product/delete", req.headers.get("Authorization"), req.get_json())

# ===== GATEWAY → INVENTORY SERVICE =====
@app.route(route="inventory", auth_level=func.AuthLevel.FUNCTION)
def gw_inventory(req: func.HttpRequest):
    claims, resp = require_user(req)
    if resp: return resp
    return proxy("GET", INVENTORY_SERVICE_URL, "/inventory", req.headers.get("Authorization"))

@app.route(route="inventory/create", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def gw_create_inventory(req: func.HttpRequest):
    claims, resp = require_user(req)
    if resp: return resp
    if not require_role(claims, ["Owner"]): return error("owner_only", 403)
    return proxy("POST", INVENTORY_SERVICE_URL, "/inventory/create", req.headers.get("Authorization"), req.get_json())

@app.route(route="inventory/update", methods=["PUT"], auth_level=func.AuthLevel.FUNCTION)
def gw_update_inventory(req: func.HttpRequest):
    claims, resp = require_user(req)
    if resp: return resp
    if not require_role(claims, ["Owner"]): return error("owner_only", 403)
    return proxy("PUT", INVENTORY_SERVICE_URL, "/inventory/update", req.headers.get("Authorization"), req.get_json())

@app.route(route="inventory/delete", methods=["DELETE"], auth_level=func.AuthLevel.FUNCTION)
def gw_delete_inventory(req: func.HttpRequest):
    claims, resp = require_user(req)
    if resp: return resp
    if not require_role(claims, ["Owner"]): return error("owner_only", 403)
    return proxy("DELETE", INVENTORY_SERVICE_URL, "/inventory/delete", req.headers.get("Authorization"), req.get_json())

# ===== GATEWAY → REPORT SERVICE =====
@app.route(route="report/run", auth_level=func.AuthLevel.FUNCTION)
def gw_report_run(req: func.HttpRequest):
    claims, resp = require_user(req)
    if resp: return resp
    return proxy("GET", REPORT_SERVICE_URL, "/report/run", req.headers.get("Authorization"))