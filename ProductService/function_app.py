import azure.functions as func
import requests
import json, os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.auth import require_role, require_user, error

app = func.FunctionApp()

DATA_FILE = "products_db.json"


def _ensure_file():
    if not os.path.exists(DATA_FILE):
        with open(DATA_FILE, "w") as f:
            json.dump({"products": []}, f)

def _load():
    _ensure_file()
    with open(DATA_FILE, "r") as f:
        return json.load(f)

def _save(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=4)


# ========= READ (tenant-scoped) =========
@app.route(route="product/products", auth_level=func.AuthLevel.FUNCTION)
def get_products(req: func.HttpRequest) -> func.HttpResponse:
    claims, err = require_user(req)
    if err: 
        return err

    tenant = claims["tenantId"]
    data = _load()

    # Item lama (seed) mungkin belum punya tenantId -> treat as public template
    tenant_products = [
        p for p in data["products"] 
        if p.get("tenantId") in (None, tenant)
    ]

    return func.HttpResponse(json.dumps(tenant_products), mimetype="application/json")


# ========= MANAGE (RBAC: Owner only) =========
@app.route(route="product/manage", auth_level=func.AuthLevel.FUNCTION)
def manage(req: func.HttpRequest) -> func.HttpResponse:
    claims, err = require_user(req)
    if err: 
        return err
    if not require_role(claims, ["Owner"]):
        return error("owner_only", 403)

    return func.HttpResponse(
        json.dumps({"permission": ["Tambah produk", "Edit", "Delete"]}, indent=2),
        mimetype="application/json"
    )


# ========= CREATE =========
@app.route(route="product/create", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def create_product(req: func.HttpRequest) -> func.HttpResponse:
    claims, err = require_user(req)
    if err: 
        return err
    if not require_role(claims, ["Owner"]):
        return error("owner_only", 403)

    data = _load()
    try:
        body = req.get_json()
    except Exception as e:
        return func.HttpResponse(f"Bad Request: {e}", status_code=400)

    # id incremental
    prev_product_id = int(data['inventories'][-1]['inventory_id'].lstrip("P"))
    next_product_id = f"P{(prev_product_id + 1):03d}"
    new_product = {
        "product_id": next_product_id,
        "name": body['name'],
        "description": body['description'],
        "price": body['price'],
        "tenantId": claims['tenantId']
    }

    data["products"].append(new_product)
    _save(data)

    return func.HttpResponse(json.dumps(new_product), mimetype="application/json")


# ========= UPDATE =========
@app.route(route="product/update", methods=["PUT"], auth_level=func.AuthLevel.FUNCTION)
def update_product(req: func.HttpRequest) -> func.HttpResponse:
    claims, err = require_user(req)
    if err: 
        return err
    if not require_role(claims, ["Owner"]):
        return error("owner_only", 403)

    try:
        body = req.get_json()
    except Exception as e:
        return func.HttpResponse(f"Bad Request: {e}", status_code=400)

    data = _load()
    for i, p in enumerate(data["products"]):
        if p["product_id"] == body["product_id"] and p["tenantId"] == claims["tenantId"]:
            # kunci tenantId agar tidak bisa dipindahkan ke tenant lain
            body["tenantId"] = claims["tenantId"]
            data["products"][i] = body
            _save(data)
            return func.HttpResponse(json.dumps(body), mimetype="application/json")

    return error("not found", 404)


# ========= DELETE =========
@app.route(route="product/delete", methods=["DELETE"], auth_level=func.AuthLevel.FUNCTION)
def delete_product(req: func.HttpRequest) -> func.HttpResponse:
    claims, err = require_user(req)
    if err: 
        return err
    if not require_role(claims, ["Owner"]):
        return error("owner_only", 403)

    try:
        body = req.get_json()
    except Exception as e:
        return func.HttpResponse(f"Bad Request: {e}", status_code=400)

    data = _load()
    before = len(data["products"])
    data["products"] = [
        p for p in data["products"]
        if not (p["product_id"] == body["product_id"] and p["tenantId"] == claims["tenantId"])
    ]
    _save(data)

    if len(data["products"]) == before:
        return error("not_found_or_forbidden", 404)

    return func.HttpResponse(f"Deleted {body['product_id']}", mimetype="text/plain")
