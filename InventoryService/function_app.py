import azure.functions as func
import datetime
import json
import logging
import requests
import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.auth import require_role, require_user, error

app = func.FunctionApp()

DATA_FILE = "inventories_db.json"

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

@app.route(route="inventory", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def get_inventory(req: func.HttpRequest) -> func.HttpResponse:
    claims, err = require_user(req)
    if err:
        return err
    
    if not require_role(claims, ["Owner"]):
        return error("forbidden: owner only", 403)
    
    tenant = claims["tenantId"]
    data = _load()

    tenant_inventory = [
        p for p in data["inventories"] 
        if p.get("tenantId") in (None, tenant)
    ]

    try:
        products = requests.get(
            "http://localhost:7072/api/product/products",
            headers={
                "Authorization": req.headers.get("Authorization"),
            }
        ).json()
    except Exception as e:
        return func.HttpResponse(e)

    product_map = {p["product_id"]: p for p in products}

    for row in tenant_inventory:
        product = product_map.get(row["product_id"])
        row["product"] = product
    
    return func.HttpResponse(json.dumps(tenant_inventory), status_code=200, mimetype="application/json")

@app.route(route="inventory/create", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def create_inventory(req: func.HttpRequest) -> func.HttpResponse:
    claims, err = require_user(req)
    if err:
        return err
    
    if not require_role(claims, ["Owner"]):
        return error("forbidden: owner only", 403)
    
    try:
        body = req.get_json()
    except Exception as e:
        return func.HttpResponse(f"Bad Request: {e}", status_code=400)

    data = _load()

    prev_inventory_id = int(data['inventories'][-1]['inventory_id'].lstrip("I"))
    next_inventory_id = f"I{(prev_inventory_id + 1):03d}"
    new_inventory_item = {
        "inventory_id": next_inventory_id,
        "product_id": body['product_id'],
        "available_qty": body['available_qty'],
        "sold_qty": 0,
        "reserved_qty": 0,
        "tenantId": claims['tenantId']
    }

    data['inventories'].append(new_inventory_item)
    _save(data)
    
    return func.HttpResponse(json.dumps(new_inventory_item), status_code=200, mimetype="application/json")

@app.route(route="inventory/update", methods=["PUT"], auth_level=func.AuthLevel.FUNCTION)
def update_inventory(req: func.HttpRequest) -> func.HttpResponse:
    claims, err = require_user(req)
    if err:
        return err
    
    if not require_role(claims, ["Owner"]):
        return error("forbidden: owner only", 403)
    
    try:
        body = req.get_json()
        body['tenantId'] = claims['tenantId']
    except Exception as e:
        return func.HttpResponse(f"Bad Request: {e}", status_code=400)

    data = _load()
    i = 0
    for row in data['inventories']:
        if row["inventory_id"] == body['inventory_id'] and row["tenantId"] == claims["tenantId"]:
            data['inventories'][i] = body
            _save(data)
            return func.HttpResponse(json.dumps(body), status_code=200, mimetype="application/json")

        i += 1
    
    return error("not found", 404)

@app.route(route="inventory/delete", methods=["DELETE"], auth_level=func.AuthLevel.FUNCTION)
def delete_inventory(req: func.HttpRequest) -> func.HttpResponse:
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
    before = len(data["inventories"])
    data["inventories"] = [
        p for p in data["inventories"]
        if not (p["inventory_id"] == body["inventory_id"] and p["tenantId"] == claims["tenantId"])
    ]
    _save(data)

    if len(data["inventories"]) == before:
        return error("NOT FOUND", 404)

    return func.HttpResponse(f"Deleted {body['inventory_id']}", mimetype="text/plain")