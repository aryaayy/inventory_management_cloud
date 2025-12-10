import azure.functions as func
from azure.cosmos import CosmosClient, exceptions
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import logging
import json
import os
import uuid
import datetime
import base64

app = func.FunctionApp()

# --- KONFIGURASI ENVIRONMENT ---
ENDPOINT = os.environ.get("COSMOS_ENDPOINT")
KEY = os.environ.get("COSMOS_KEY")
DATABASE_NAME = os.environ.get("COSMOS_DATABASE")
CONTAINER_NAME = os.environ.get("COSMOS_CONTAINER")

# Variable baru untuk Blob
BLOB_CONN_STR = os.environ.get("BLOB_CONNECTION_STRING")
BLOB_CONTAINER = "product-images"

# Variable untuk Service Bus
SB_CONN_STR = os.environ.get("SERVICE_BUS_CONNECTION")
TOPIC_NAME = "product-events"

client = None
container = None
blob_service_client = None

def get_container():
    global client, container
    if not container:
        try:
            client = CosmosClient(ENDPOINT, KEY)
            database = client.get_database_client(DATABASE_NAME)
            container = database.get_container_client(CONTAINER_NAME)
        except Exception as e:
            logging.error(f"Error connecting to Cosmos DB: {e}")
            raise e
    return container

def get_blob_service():
    global blob_service_client
    if not blob_service_client and BLOB_CONN_STR:
        try:
            blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
        except Exception as e:
            logging.error(f"Error connecting to Blob Storage: {e}")
            # Jangan raise error di sini biar app tetap jalan meski storage bermasalah
    return blob_service_client

def get_iso_timestamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

# --- FUNGSI BARU: UPLOAD KE BLOB ---
def process_images(image_list):
    """
    Menerima list gambar. 
    - Kalau formatnya Base64 -> Upload ke Blob -> Ganti jadi URL.
    - Kalau formatnya sudah URL -> Biarkan saja.
    """
    if not image_list: return []
    
    clean_urls = []
    blob_client_service = get_blob_service()
    
    for img in image_list:
        # 1. Jika sudah URL (misal edit produk tapi gambar gak diganti), skip upload
        if img.startswith("http"):
            clean_urls.append(img)
            continue
            
        # 2. Jika Base64, Upload!
        if img.startswith("data:image") and blob_client_service:
            try:
                # Parsing: "data:image/jpeg;base64,....."
                header, encoded = img.split(",", 1)
                file_ext = header.split(";")[0].split("/")[1] # dapat 'jpeg' atau 'png'
                
                # Decode jadi binary
                data = base64.b64decode(encoded)
                
                # Nama file unik: uuid.jpg
                filename = f"{uuid.uuid4()}.{file_ext}"
                
                # Upload
                blob_client = blob_client_service.get_blob_client(container=BLOB_CONTAINER, blob=filename)
                blob_client.upload_blob(
                    data, 
                    overwrite=True,
                    content_settings=ContentSettings(content_type=f"image/{file_ext}")
                )
                
                # Ambil URL Hasil Upload
                clean_urls.append(blob_client.url)
                logging.info(f"Success upload blob: {blob_client.url}")
                
            except Exception as e:
                logging.error(f"Failed to upload image: {e}")
                # Kalau gagal, jangan crash, skip aja gambarnya
        else:
            # Kalau format string aneh, skip
            continue
            
    return clean_urls

def publish_event(sku, action, data=None):
    """
    Mengirim pesan ke Service Bus Topic.
    Action: 'PRODUCT_CREATED', 'PRODUCT_UPDATED', 'STOCK_CHANGED'
    """
    if not SB_CONN_STR:
        logging.warning("Service Bus Connection String not found. Skipping publish.")
        return

    try:
        # Payload pesan
        message_body = {
            "sku": sku,
            "action": action,
            "timestamp": get_iso_timestamp(),
            "data": data # Data ringkas saja (misal: harga baru, atau stok baru)
        }
        
        # Kirim Pesan
        servicebus_client = ServiceBusClient.from_connection_string(conn_str=SB_CONN_STR, logging_enable=True)
        with servicebus_client:
            sender = servicebus_client.get_topic_sender(topic_name=TOPIC_NAME)
            with sender:
                message = ServiceBusMessage(json.dumps(message_body))
                # Kita bisa tambah Application Properties untuk filter (opsional)
                message.application_properties = {'messagetype': 'product_update'} 
                sender.send_messages(message)
                
        logging.info(f"Event {action} for {sku} published to Service Bus.")
        
    except Exception as e:
        logging.error(f"Failed to publish event: {str(e)}")

# ==========================================
# 1. CREATE PRODUCT (Updated with Blob)
# ==========================================
@app.route(route="product/create", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def create_product(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Create Product request.')
    
    try:
        req_body = req.get_json()
        ctr = get_container()
    except ValueError:
        return func.HttpResponse("Invalid JSON body", status_code=400)

    if 'sku' not in req_body or 'name' not in req_body:
        return func.HttpResponse("Field 'sku' dan 'name' wajib ada.", status_code=400)

    # Cek Unik SKU
    query = "SELECT * FROM c WHERE c.sku = @sku"
    existing = list(ctr.query_items(query=query, parameters=[{"name": "@sku", "value": req_body['sku']}], enable_cross_partition_query=True))
    if existing:
        return func.HttpResponse(f"Product dengan SKU {req_body['sku']} sudah ada.", status_code=409)

    new_id = str(uuid.uuid4())
    timestamp = get_iso_timestamp()

    # --- PROSES GAMBAR (BLOB) ---
    raw_images = req_body.get('images', [])
    final_images = process_images(raw_images) # Upload ke Blob & Dapatkan URL
    # ----------------------------

    warehouses = req_body.get('warehouses', [])
    total_quantity = 0
    for warehouse in warehouses:
        total_quantity += warehouse.get('quantity')
    
    inventory_summary = {
        "total_quantity": total_quantity,
        "last_stock_update": timestamp
    }

    new_product = {
        "id": new_id,   # PARTITION KEY
        "sku": req_body['sku'], 
        "name": req_body['name'],
        "description": req_body.get('description', ""),
        "brand": req_body.get('brand', "No Brand"),
        "base_price": req_body.get('base_price', 0),
        "status": req_body.get('status', 'ACTIVE'),
        
        "images": final_images, # Simpan URL Blob, bukan Base64!
        
        "inventory_summary": inventory_summary,
        "warehouses": req_body.get('warehouses', []),
        "connected_channels": req_body.get('connected_channels', []), 
        "created_at": timestamp,
        "updated_at": timestamp
    }

    try:
        ctr.create_item(body=new_product)

        publish_event(
            sku=new_product['sku'], 
            action="PRODUCT_CREATED", 
            data=new_product
        )
        logging.info(f"Produk {new_product['sku']} berhasil dibuat. Menunggu sinkronisasi...")

        return func.HttpResponse(json.dumps(new_product), mimetype="application/json", status_code=201)

    except exceptions.CosmosHttpResponseError as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


# ==========================================
# 2. READ PRODUCT(S) (Sama Saja)
# ==========================================
@app.route(route="product/products", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_products(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Get Products request.')
    
    ctr = get_container()
    sku_filter = req.params.get('sku')
    id_filter = req.params.get('id')

    try:
        if id_filter:
            try:
                item = ctr.read_item(item=id_filter, partition_key=id_filter)
                items = [item]
            except exceptions.CosmosResourceNotFoundError:
                items = []
        elif sku_filter:
            query = "SELECT * FROM c WHERE c.sku = @sku"
            items = list(ctr.query_items(query=query, parameters=[{"name": "@sku", "value": sku_filter}], enable_cross_partition_query=True))
        else:
            query = "SELECT * FROM c"
            items = list(ctr.query_items(query=query, enable_cross_partition_query=True))

        return func.HttpResponse(json.dumps(items), mimetype="application/json", status_code=200)

    except exceptions.CosmosHttpResponseError as e:
        return func.HttpResponse(f"Error reading DB: {e}", status_code=500)


# ==========================================
# 3. UPDATE PRODUCT (Updated with Blob)
# ==========================================
@app.route(route="product/update", methods=["PUT"], auth_level=func.AuthLevel.ANONYMOUS)
def update_product(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Update Product request.')

    try:
        req_body = req.get_json()
        ctr = get_container()
    except ValueError:
        return func.HttpResponse("Invalid JSON body", status_code=400)

    if 'id' not in req_body:
        return func.HttpResponse("Field 'id' wajib ada untuk update.", status_code=400)

    item_id = req_body['id']

    try:
        existing_item = ctr.read_item(item=item_id, partition_key=item_id)

        # --- PROSES GAMBAR (UPDATE) ---
        if 'images' in req_body:
            # Fungsi process_images sudah handle mix antara URL lama dan Base64 baru
            existing_item['images'] = process_images(req_body['images'])
        # ------------------------------

        # Update field lain (gunakan .get untuk safety)
        existing_item['name'] = req_body.get('name', existing_item.get('name'))
        existing_item['description'] = req_body.get('description', existing_item.get('description'))
        existing_item['brand'] = req_body.get('brand', existing_item.get('brand'))
        existing_item['base_price'] = req_body.get('base_price', existing_item.get('base_price'))
        existing_item['status'] = req_body.get('status', existing_item.get('status'))
        
        # Connected channels & Warehouses
        existing_item['connected_channels'] = req_body.get('connected_channels', existing_item.get('connected_channels'))
        
        if 'warehouses' in req_body:
            new_warehouses = req_body['warehouses']
            existing_item['warehouses'] = new_warehouses
            
            # Recalculate stock
            total_quantity = 0
            for warehouse in new_warehouses:
                total_quantity += warehouse.get('quantity', 0)
            
            existing_item['inventory_summary'] = {
                "total_quantity": total_quantity,
                "last_stock_update": get_iso_timestamp()
            }

        existing_item['updated_at'] = get_iso_timestamp()

        updated_item = ctr.replace_item(item=item_id, body=existing_item)

        # Publish Update Event
        publish_event(
            sku=updated_item['sku'], 
            action="PRODUCT_UPDATED", 
            data=updated_item
        )
        logging.info(f"Produk {updated_item['sku']} berhasil diupdate.")

        return func.HttpResponse(json.dumps(updated_item), mimetype="application/json", status_code=200)

    except exceptions.CosmosResourceNotFoundError:
        return func.HttpResponse("Produk tidak ditemukan.", status_code=404)
    except exceptions.CosmosHttpResponseError as e:
        return func.HttpResponse(f"Error updating DB: {e}", status_code=500)


# ==========================================
# 4. DELETE PRODUCT (Sama Saja)
# ==========================================
@app.route(route="product/delete", methods=["DELETE"], auth_level=func.AuthLevel.ANONYMOUS)
def delete_product(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Delete Product request.')

    item_id = req.params.get('id')
    if not item_id:
        return func.HttpResponse("Parameter 'id' wajib ada.", status_code=400)

    ctr = get_container()

    try:
        ctr.delete_item(item=item_id, partition_key=item_id)
        logging.info(f"Produk {item_id} berhasil dihapus.")
        return func.HttpResponse(f"Produk dengan ID {item_id} berhasil dihapus.", status_code=200)

    except exceptions.CosmosResourceNotFoundError:
        return func.HttpResponse("Produk tidak ditemukan.", status_code=404)
    except exceptions.CosmosHttpResponseError as e:
        return func.HttpResponse(f"Error deleting from DB: {e}", status_code=500)

# import azure.functions as func
# from azure.cosmos import CosmosClient, PartitionKey, exceptions
# import requests
# import uuid
# import logging
# import json, os, sys
# # from auth_utils import require_role, require_user, error

# app = func.FunctionApp()

# ENDPOINT = os.environ.get("COSMOS_ENDPOINT")
# KEY = os.environ.get("COSMOS_KEY")
# DATABASE = os.environ.get("COSMOS_DATABASE")
# CONTAINER = os.environ.get("COSMOS_CONTAINER")

# client = None
# container = None

# def get_container():
#     """
#     Fungsi ini membuat koneksi hanya SAAT DIBUTUHKAN.
#     Mencegah aplikasi mati duluan jika koneksi gagal saat startup.
#     """
#     global client, container
#     if not container:
#         logging.info("Mencoba menghubungkan ke Cosmos DB...")
#         try:
#             # Validasi Variable Wajib
#             if not ENDPOINT or not KEY:
#                 raise ValueError("FATAL: COSMOS_ENDPOINT atau COSMOS_KEY kosong/tidak terbaca!")
            
#             client = CosmosClient(ENDPOINT, KEY)
#             database = client.get_database_client(DATABASE)
#             container = database.get_container_client(CONTAINER)
#             logging.info(f"Berhasil konek ke container: {CONTAINER}")
#         except Exception as e:
#             logging.error(f"Gagal inisialisasi DB: {str(e)}")
#             raise e # Lempar error agar kelihatan di log saat fungsi dipanggil
#     return container

# DATA_FILE = "products_db.json"

# def _ensure_file():
#     if not os.path.exists(DATA_FILE):
#         with open(DATA_FILE, "w") as f:
#             json.dump({"products": []}, f)

# def _load():
#     _ensure_file()
#     with open(DATA_FILE, "r") as f:
#         return json.load(f)

# def _save(data):
#     with open(DATA_FILE, "w") as f:
#         json.dump(data, f, indent=4)


# # ========= READ (tenant-scoped) =========
# @app.route(route="product/products", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
# def get_products(req: func.HttpRequest) -> func.HttpResponse:
#     try:
#         ctr = get_container()

#         query = """
#             SELECT * FROM c 
#         """

#         items = list(ctr.query_items(
#             query=query,
#             enable_cross_partition_query=True
#         ))

#         return func.HttpResponse(json.dumps(items), mimetype="application/json")
    
#     except exceptions.CosmosHttpResponseError as e:
#         return func.HttpResponse(f"DB Error: {str(e)}", status_code=500)

# # ========= CREATE =========
# @app.route(route="product/create", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
# def create_product(req: func.HttpRequest) -> func.HttpResponse:
#     # claims, err = require_user(req)
#     # if err: return err
#     # if not require_role(claims, ["Owner"]):
#     #     return error("owner_only", 403)

#     try:
#         ctr = get_container()
#         body = req.get_json()
#     except ValueError:
#         return func.HttpResponse("Invalid JSON", status_code=400)

#     # Best Practice Cosmos: Gunakan UUID untuk ID unik.
#     # Incremental ID (P001, P002) rawan "Race Condition" di cloud.
#     new_id = str(uuid.uuid4())

#     new_product = {
#         "id": new_id,  # Cosmos DB mewajibkan field bernama 'id' (string)
#         "name": body.get('name'),
#         "init_stock": body.get('init_stock'),
#         "price": body.get('price'),
#     }

#     try:
#         ctr.create_item(body=new_product)
#         response = requests.post("https://sync-service-k3.azurewebsites.net/api/sync_marketplace_item", json=new_product)
#         return func.HttpResponse(json.dumps(new_product), mimetype="application/json", status_code=201)
#     except exceptions.CosmosHttpResponseError as e:
#         return func.HttpResponse(f"Failed to create: {str(e)}", status_code=500)

# # @app.route(route="product/products", auth_level=func.AuthLevel.FUNCTION)
# # def get_products(req: func.HttpRequest) -> func.HttpResponse:
# #     claims, err = require_user(req)
# #     if err: 
# #         return err

# #     tenant = claims["tenantId"]
# #     data = _load()

# #     # Item lama (seed) mungkin belum punya tenantId -> treat as public template
# #     tenant_products = [
# #         p for p in data["products"] 
# #         if p.get("tenantId") in (None, tenant)
# #     ]

# #     return func.HttpResponse(json.dumps(tenant_products), mimetype="application/json")


# # # ========= MANAGE (RBAC: Owner only) =========
# # @app.route(route="product/manage", auth_level=func.AuthLevel.FUNCTION)
# # def manage(req: func.HttpRequest) -> func.HttpResponse:
# #     claims, err = require_user(req)
# #     if err: 
# #         return err
# #     if not require_role(claims, ["Owner"]):
# #         return error("owner_only", 403)

# #     return func.HttpResponse(
# #         json.dumps({"permission": ["Tambah produk", "Edit", "Delete"]}, indent=2),
# #         mimetype="application/json"
# #     )


# # # ========= CREATE =========
# # @app.route(route="product/create", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
# # def create_product(req: func.HttpRequest) -> func.HttpResponse:
# #     claims, err = require_user(req)
# #     if err: 
# #         return err
# #     if not require_role(claims, ["Owner"]):
# #         return error("owner_only", 403)

# #     data = _load()
# #     try:
# #         body = req.get_json()
# #     except Exception as e:
# #         return func.HttpResponse(f"Bad Request: {e}", status_code=400)

# #     # id incremental
# #     prev_product_id = int(data['inventories'][-1]['inventory_id'].lstrip("P"))
# #     next_product_id = f"P{(prev_product_id + 1):03d}"
# #     new_product = {
# #         "product_id": next_product_id,
# #         "name": body['name'],
# #         "description": body['description'],
# #         "price": body['price'],
# #         "tenantId": claims['tenantId']
# #     }

# #     data["products"].append(new_product)
# #     _save(data)

# #     return func.HttpResponse(json.dumps(new_product), mimetype="application/json")


# # # ========= UPDATE =========
# # @app.route(route="product/update", methods=["PUT"], auth_level=func.AuthLevel.FUNCTION)
# # def update_product(req: func.HttpRequest) -> func.HttpResponse:
# #     claims, err = require_user(req)
# #     if err: 
# #         return err
# #     if not require_role(claims, ["Owner"]):
# #         return error("owner_only", 403)

# #     try:
# #         body = req.get_json()
# #     except Exception as e:
# #         return func.HttpResponse(f"Bad Request: {e}", status_code=400)

# #     data = _load()
# #     for i, p in enumerate(data["products"]):
# #         if p["product_id"] == body["product_id"] and p["tenantId"] == claims["tenantId"]:
# #             # kunci tenantId agar tidak bisa dipindahkan ke tenant lain
# #             body["tenantId"] = claims["tenantId"]
# #             data["products"][i] = body
# #             _save(data)
# #             return func.HttpResponse(json.dumps(body), mimetype="application/json")

# #     return error("not found", 404)


# # # ========= DELETE =========
# # @app.route(route="product/delete", methods=["DELETE"], auth_level=func.AuthLevel.FUNCTION)
# # def delete_product(req: func.HttpRequest) -> func.HttpResponse:
# #     claims, err = require_user(req)
# #     if err: 
# #         return err
# #     if not require_role(claims, ["Owner"]):
# #         return error("owner_only", 403)

# #     try:
# #         body = req.get_json()
# #     except Exception as e:
# #         return func.HttpResponse(f"Bad Request: {e}", status_code=400)

# #     data = _load()
# #     before = len(data["products"])
# #     data["products"] = [
# #         p for p in data["products"]
# #         if not (p["product_id"] == body["product_id"] and p["tenantId"] == claims["tenantId"])
# #     ]
# #     _save(data)

# #     if len(data["products"]) == before:
# #         return error("not_found_or_forbidden", 404)

# #     return func.HttpResponse(f"Deleted {body['product_id']}", mimetype="text/plain")
