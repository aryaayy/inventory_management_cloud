import azure.functions as func
import datetime
import json
import logging
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.auth import require_role, require_user, error

app = func.FunctionApp()

# ---------------- PUBLIC PAGES ----------------
@app.route(route="product/products", auth_level=func.AuthLevel.ANONYMOUS)
def get_products(req: func.HttpRequest) -> func.HttpResponse:
    with open("products_db.json", "r") as f:
        data = json.load(f)

    return func.HttpResponse(json.dumps(data), mimetype="application/json")

@app.route(route="product/create", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def create_product(req: func.HttpRequest) -> func.HttpResponse:
    claims, err = require_user(req)
    if err:
        return err
    
    if not require_role(claims, ["Owner"]):
        return error("forbidden: owner only", 403)

    with open("products_db.json", "r") as f:
        data = json.load(f)
    
    try:
        new_product = req.get_json()
    except Exception as e:
        return func.HttpResponse(f"Bad Request: {e}", status_code=400)

    new_product["id"] = f"P{(len(data["products"]) + 1):03d}"
    data["products"].append(new_product)

    with open("products_db.json", "w") as f:
        json.dump(data, f, indent=4)

    return func.HttpResponse(json.dumps(new_product), status_code=200, mimetype="application/json")

@app.route(route="product/update", methods=["PUT"], auth_level=func.AuthLevel.FUNCTION)
def update_product(req: func.HttpRequest) -> func.HttpResponse:
    claims, err = require_user(req)
    if err:
        return err
    
    if not require_role(claims, ["Owner"]):
        return error("forbidden: owner only", 403)

    with open("products_db.json", "r") as f:
        data = json.load(f)
    
    try:
        updated_product = req.get_json()
    except Exception as e:
        return func.HttpResponse(f"Bad Request: {e}", status_code=400)

    i = 0
    for row in data["products"]:
        if row["id"] == updated_product["id"]:
            break
        i += 1
    
    if i > len(data["products"]) - 1:
        return func.HttpResponse(f"Data tidak ditemukan", status_code=400)
    
    data["products"][i] = updated_product

    with open("products_db.json", "w") as f:
        json.dump(data, f, indent=4)

    return func.HttpResponse(json.dumps(updated_product), status_code=200, mimetype="application/json")

@app.route(route="product/delete", methods=["DELETE"], auth_level=func.AuthLevel.FUNCTION)
def delete_product(req: func.HttpRequest) -> func.HttpResponse:
    claims, err = require_user(req)
    if err:
        return err
    
    if not require_role(claims, ["Owner"]):
        return error("forbidden: owner only", 403)

    with open("products_db.json", "r") as f:
        data = json.load(f)
    
    try:
        body = req.get_json()
    except Exception as e:
        return func.HttpResponse(f"Bad Request: {e}", status_code=400)
        
    data["products"] = [row for row in data["products"] if row["id"] != body["id"]]

    with open("products_db.json", "w") as f:
        json.dump(data, f, indent=4)

    return func.HttpResponse(f"Successfully deleted {body["id"]}", status_code=200, mimetype="application/json")

# Product management (Owner only)
@app.route(route="product/manage", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def manage_products(req: func.HttpRequest) -> func.HttpResponse:
    claims, err = require_user(req)
    if err: return err

    if not require_role(claims, ["Owner"]):
        return error("forbidden: owner only", 403)

    return func.HttpResponse(
        json.dumps({"permission": ["Tambah produk", "Edit", "Delete"]}, indent=2),
        mimetype="application/json"
    )