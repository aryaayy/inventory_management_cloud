import azure.functions as func
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import requests
import uuid
import json, os, sys
from auth_utils import require_role, require_user, error

app = func.FunctionApp()

ENDPOINT = os.environ.get("COSMOS_ENDPOINT")
KEY = os.environ.get("COSMOS_KEY")
DATABASE = os.environ.get("COSMOS_DATABASE")
CONTAINER = os.environ.get("COSMOS_CONTAINER")

client = CosmosClient(ENDPOINT, KEY)
database = client.get_database_client(DATABASE)
container = database.get_container_client(CONTAINER)

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
    try:
        query = """
            SELECT * FROM c 
        """

        items = list(container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))

        return func.HttpResponse(json.dumps(items), mimetype="application/json")
    
    except exceptions.CosmosHttpResponseError as e:
        return func.HttpResponse(f"DB Error: {str(e)}", status_code=500)

# ========= CREATE =========
@app.route(route="product/create", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def create_product(req: func.HttpRequest) -> func.HttpResponse:
    # claims, err = require_user(req)
    # if err: return err
    # if not require_role(claims, ["Owner"]):
    #     return error("owner_only", 403)

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    # Best Practice Cosmos: Gunakan UUID untuk ID unik.
    # Incremental ID (P001, P002) rawan "Race Condition" di cloud.
    new_id = str(uuid.uuid4())

    new_product = {
        "id": new_id,  # Cosmos DB mewajibkan field bernama 'id' (string)
        "name": body.get('name'),
        "init_stock": body.get('init_stock'),
        "price": body.get('price'),
    }

    try:
        container.create_item(body=new_product)
        response = requests.post("http://localhost:7001/api/sync_marketplace_item", json=new_product)
        return func.HttpResponse(json.dumps(new_product), mimetype="application/json", status_code=201)
    except exceptions.CosmosHttpResponseError as e:
        return func.HttpResponse(f"Failed to create: {str(e)}", status_code=500)

# @app.route(route="product/products", auth_level=func.AuthLevel.FUNCTION)
# def get_products(req: func.HttpRequest) -> func.HttpResponse:
#     claims, err = require_user(req)
#     if err: 
#         return err

#     tenant = claims["tenantId"]
#     data = _load()

#     # Item lama (seed) mungkin belum punya tenantId -> treat as public template
#     tenant_products = [
#         p for p in data["products"] 
#         if p.get("tenantId") in (None, tenant)
#     ]

#     return func.HttpResponse(json.dumps(tenant_products), mimetype="application/json")


# # ========= MANAGE (RBAC: Owner only) =========
# @app.route(route="product/manage", auth_level=func.AuthLevel.FUNCTION)
# def manage(req: func.HttpRequest) -> func.HttpResponse:
#     claims, err = require_user(req)
#     if err: 
#         return err
#     if not require_role(claims, ["Owner"]):
#         return error("owner_only", 403)

#     return func.HttpResponse(
#         json.dumps({"permission": ["Tambah produk", "Edit", "Delete"]}, indent=2),
#         mimetype="application/json"
#     )


# # ========= CREATE =========
# @app.route(route="product/create", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
# def create_product(req: func.HttpRequest) -> func.HttpResponse:
#     claims, err = require_user(req)
#     if err: 
#         return err
#     if not require_role(claims, ["Owner"]):
#         return error("owner_only", 403)

#     data = _load()
#     try:
#         body = req.get_json()
#     except Exception as e:
#         return func.HttpResponse(f"Bad Request: {e}", status_code=400)

#     # id incremental
#     prev_product_id = int(data['inventories'][-1]['inventory_id'].lstrip("P"))
#     next_product_id = f"P{(prev_product_id + 1):03d}"
#     new_product = {
#         "product_id": next_product_id,
#         "name": body['name'],
#         "description": body['description'],
#         "price": body['price'],
#         "tenantId": claims['tenantId']
#     }

#     data["products"].append(new_product)
#     _save(data)

#     return func.HttpResponse(json.dumps(new_product), mimetype="application/json")


# # ========= UPDATE =========
# @app.route(route="product/update", methods=["PUT"], auth_level=func.AuthLevel.FUNCTION)
# def update_product(req: func.HttpRequest) -> func.HttpResponse:
#     claims, err = require_user(req)
#     if err: 
#         return err
#     if not require_role(claims, ["Owner"]):
#         return error("owner_only", 403)

#     try:
#         body = req.get_json()
#     except Exception as e:
#         return func.HttpResponse(f"Bad Request: {e}", status_code=400)

#     data = _load()
#     for i, p in enumerate(data["products"]):
#         if p["product_id"] == body["product_id"] and p["tenantId"] == claims["tenantId"]:
#             # kunci tenantId agar tidak bisa dipindahkan ke tenant lain
#             body["tenantId"] = claims["tenantId"]
#             data["products"][i] = body
#             _save(data)
#             return func.HttpResponse(json.dumps(body), mimetype="application/json")

#     return error("not found", 404)


# # ========= DELETE =========
# @app.route(route="product/delete", methods=["DELETE"], auth_level=func.AuthLevel.FUNCTION)
# def delete_product(req: func.HttpRequest) -> func.HttpResponse:
#     claims, err = require_user(req)
#     if err: 
#         return err
#     if not require_role(claims, ["Owner"]):
#         return error("owner_only", 403)

#     try:
#         body = req.get_json()
#     except Exception as e:
#         return func.HttpResponse(f"Bad Request: {e}", status_code=400)

#     data = _load()
#     before = len(data["products"])
#     data["products"] = [
#         p for p in data["products"]
#         if not (p["product_id"] == body["product_id"] and p["tenantId"] == claims["tenantId"])
#     ]
#     _save(data)

#     if len(data["products"]) == before:
#         return error("not_found_or_forbidden", 404)

#     return func.HttpResponse(f"Deleted {body['product_id']}", mimetype="text/plain")
