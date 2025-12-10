import azure.functions as func
from azure.cosmos import CosmosClient, exceptions
import logging
import json
import os
import uuid
import datetime
from azure.servicebus import ServiceBusClient, ServiceBusMessage
        
app = func.FunctionApp()

# Konfigurasi Environment
ENDPOINT = os.environ.get("COSMOS_ENDPOINT")
KEY = os.environ.get("COSMOS_KEY")
DATABASE_NAME = os.environ.get("COSMOS_DATABASE")
CONTAINER_NAME = os.environ.get("COSMOS_CONTAINER")

client = None
container = None

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

# Ambil Connection String dari local.settings.json
SB_CONN_STR = os.environ.get("SERVICE_BUS_CONNECTION")
TOPIC_NAME = "product-events"

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

def get_iso_timestamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

# ==========================================
# 1. CREATE PRODUCT
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

    # Validasi Unik SKU (Opsional tapi disarankan)
    # Karena Partition Key = id, pencarian by SKU menjadi Cross-Partition Query
    query = "SELECT * FROM c WHERE c.sku = @sku"
    parameters = [{"name": "@sku", "value": req_body['sku']}]
    existing = list(ctr.query_items(query=query, parameters=parameters, enable_cross_partition_query=True))
    
    if existing:
        return func.HttpResponse(f"Product dengan SKU {req_body['sku']} sudah ada.", status_code=409)

    # Generate ID (Karena ID adalah Partition Key, ini menentukan lokasi data)
    new_id = str(uuid.uuid4())
    timestamp = get_iso_timestamp()

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
        "description": req_body['description'],
        "brand": req_body['brand'],
        "base_price": req_body.get('base_price', 0),
        "status": req_body.get('status', 'ACTIVE'),
        "images": req_body.get('images', []),
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
# 2. READ PRODUCT(S)
# ==========================================
@app.route(route="product/products", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_products(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Get Products request.')
    
    ctr = get_container()
    sku_filter = req.params.get('sku')
    id_filter = req.params.get('id')

    try:
        if id_filter:
            # Point Read (Sangat Cepat & Murah karena ID = Partition Key)
            # Menggunakan read_item lebih efisien daripada query SELECT
            try:
                item = ctr.read_item(item=id_filter, partition_key=id_filter)
                items = [item]
            except exceptions.CosmosResourceNotFoundError:
                items = []
        
        elif sku_filter:
            # Query by SKU (Cross-Partition karena Partition Key kita ID)
            query = "SELECT * FROM c WHERE c.sku = @sku"
            parameters = [{"name": "@sku", "value": sku_filter}]
            items = list(ctr.query_items(query=query, parameters=parameters, enable_cross_partition_query=True))
        
        else:
            # Get All
            query = "SELECT * FROM c"
            items = list(ctr.query_items(query=query, enable_cross_partition_query=True))

        return func.HttpResponse(json.dumps(items), mimetype="application/json", status_code=200)

    except exceptions.CosmosHttpResponseError as e:
        return func.HttpResponse(f"Error reading DB: {e}", status_code=500)


# ==========================================
# 3. UPDATE PRODUCT
# ==========================================
@app.route(route="product/update", methods=["PUT"], auth_level=func.AuthLevel.ANONYMOUS)
def update_product(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Update Product request.')

    try:
        req_body = req.get_json()
        ctr = get_container()
    except ValueError:
        return func.HttpResponse("Invalid JSON body", status_code=400)

    # Cukup ID saja karena ID adalah Partition Key
    if 'id' not in req_body:
        return func.HttpResponse("Field 'id' wajib ada untuk update.", status_code=400)

    item_id = req_body['id']

    try:
        # 1. Ambil data lama (Read Item)
        existing_item = ctr.read_item(item=item_id, partition_key=item_id)

        # 2. Update Basic Fields
        # Kita menggunakan .get(field, default_lama) agar jika user tidak mengirim field tersebut,
        # data lama tidak hilang (Partial Update).
        existing_item['name'] = req_body.get('name', existing_item.get('name'))
        existing_item['description'] = req_body.get('description', existing_item.get('description'))
        existing_item['brand'] = req_body.get('brand', existing_item.get('brand'))
        existing_item['base_price'] = req_body.get('base_price', existing_item.get('base_price'))
        existing_item['status'] = req_body.get('status', existing_item.get('status'))
        existing_item['images'] = req_body.get('images', existing_item.get('images'))
        existing_item['connected_channels'] = req_body.get('connected_channels', existing_item.get('connected_channels'))
        
        # 3. Update Warehouses & Recalculate Inventory Summary
        # Logika ini disamakan dengan Create Product
        if 'warehouses' in req_body:
            new_warehouses = req_body['warehouses']
            existing_item['warehouses'] = new_warehouses
            
            # Hitung ulang total quantity seperti di fungsi create
            total_quantity = 0
            for warehouse in new_warehouses:
                total_quantity += warehouse.get('quantity', 0)
            
            # Update object inventory_summary
            existing_item['inventory_summary'] = {
                "total_quantity": total_quantity,
                "last_stock_update": get_iso_timestamp()
            }

        # 4. Update Timestamp Terakhir
        existing_item['updated_at'] = get_iso_timestamp()

        # 5. Simpan perubahan (Replace Item)
        updated_item = ctr.replace_item(item=item_id, body=existing_item)

        # [PLACEHOLDER SINKRONISASI: Update]
        # Publish Event ke Service Bus
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
# 4. DELETE PRODUCT
# ==========================================
@app.route(route="product/delete", methods=["DELETE"], auth_level=func.AuthLevel.ANONYMOUS)
def delete_product(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Delete Product request.')

    # Ambil ID dari query param (contoh: ?id=xxx)
    item_id = req.params.get('id')

    if not item_id:
        return func.HttpResponse("Parameter 'id' wajib ada.", status_code=400)

    ctr = get_container()

    try:
        # Hapus item. partition_key diisi item_id
        ctr.delete_item(item=item_id, partition_key=item_id)

        # [PLACEHOLDER SINKRONISASI: Delete]
        logging.info(f"Produk {item_id} berhasil dihapus.")

        return func.HttpResponse(f"Produk dengan ID {item_id} berhasil dihapus.", status_code=200)

    except exceptions.CosmosResourceNotFoundError:
        return func.HttpResponse("Produk tidak ditemukan.", status_code=404)
    except exceptions.CosmosHttpResponseError as e:
        return func.HttpResponse(f"Error deleting from DB: {e}", status_code=500)

# ==========================================
# 5. SYNC STOCK FROM INVENTORY (Subscriber)
# ==========================================
@app.service_bus_topic_trigger(
    arg_name="msg", 
    topic_name="product-events", 
    subscription_name="product-stock-sub",  # <--- Subscription BARU
    connection="SERVICE_BUS_CONNECTION"
)
def process_stock_updates(msg: func.ServiceBusMessage):
    message_body = msg.get_body().decode("utf-8")
    event = json.loads(message_body)
    
    action = event.get('action')
    sku = event.get('sku')
    data = event.get('data', {})
    
    if action == "STOCK_CHANGED":
        logging.info(f"[Product] Received STOCK_CHANGED for {sku}")
        
        ctr = get_container()
        
        # 1. Cari Produk berdasarkan SKU (Cross Partition Query jika PK=/id)
        # Karena di Product Service partition key kita adalah /id, bukan /sku.
        try:
            query = "SELECT * FROM c WHERE c.sku = @sku"
            params = [{"name": "@sku", "value": sku}]
            items = list(ctr.query_items(query=query, parameters=params, enable_cross_partition_query=True))
            
            if not items:
                logging.warning(f"[Product] SKU {sku} not found via sync event.")
                return

            product_doc = items[0]
            
            # 2. Update Field Stok
            new_warehouses = data.get('warehouses', [])
            total_qty = data.get('total_available', 0)
            
            # Update struktur warehouses (Mapping kode agar sesuai schema product)
            # Pastikan formatnya cocok dengan yang diinginkan frontend Product
            mapped_warehouses = []
            for w in new_warehouses:
                mapped_warehouses.append({
                    "warehouse_code": w.get('warehouse_code'),
                    "quantity": int(w.get('quantity', 0))
                })

            product_doc['warehouses'] = mapped_warehouses
            product_doc['inventory_summary'] = {
                "total_quantity": total_qty,
                "last_stock_update": get_iso_timestamp()
            }
            
            # 3. Simpan Perubahan
            ctr.replace_item(item=product_doc['id'], body=product_doc)
            logging.info(f"[Product] Stock updated for {sku} (Total: {total_qty})")
            
        except Exception as e:
            logging.error(f"[Product] Failed to update stock: {e}")

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
