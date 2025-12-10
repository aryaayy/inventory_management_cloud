import azure.functions as func
from azure.cosmos import CosmosClient, exceptions
import logging
import json
import os
import datetime
import requests # Digunakan untuk nembak API Mock

app = func.FunctionApp()

ENDPOINT = os.environ.get("COSMOS_ENDPOINT")
KEY = os.environ.get("COSMOS_KEY")
DATABASE_NAME = os.environ.get("COSMOS_DATABASE")          # Database Internal kita
CONTAINER_NAME = os.environ.get("COSMOS_CONTAINER")    # Tempat simpan binding ID

# URL Base Mock Service (Sesuaikan port jika running local)
# Jika di Azure, ganti dengan URL Function App Mock Anda
MOCK_API_BASE_URL = os.environ.get("MOCK_API_URL")

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

def get_iso_timestamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

# ==========================================
# 1. PAYLOAD BUILDERS (Transformasi Data)
# ==========================================

def build_tokopedia_payload(data):
    # Mapping Warehouse Internal -> Tokopedia Inventory
    inv_list = []
    for w in data.get('warehouses', []):
        inv_list.append({
            "warehouse_id": w.get('warehouse_code', 'WH-DEFAULT'),
            "quantity": int(w.get('quantity', 0))
        })
    
    return {
        "title": data.get('name'),
        "status": "ACTIVATE" if data.get('status') == 'ACTIVE' else "DEACTIVATE",
        "skus": [{
            "seller_sku": data.get('sku'),
            "price": { "amount": str(data.get('base_price', 0)), "currency": "IDR" },
            "inventory": inv_list
        }],
        "main_images": [{"urls": data.get('images', [])}]
    }

def build_shopee_payload(data):
    # Hitung total stok & struktur seller stock
    total = 0
    seller_stock = []
    for w in data.get('warehouses', []):
        qty = int(w.get('quantity', 0))
        total += qty
        seller_stock.append({
            "location_id": w.get('warehouse_code', 'WH-DEFAULT'),
            "stock": qty
        })
    
    # Payload Create Item
    return {
        "item_name": data.get('name'),
        "item_sku": data.get('sku'),
        "item_status": "NORMAL",
        "original_price": int(data.get('base_price', 0)),
        "seller_stock": seller_stock, # Format simplified untuk mock add_item
        "image": { "image_url_list": data.get('images', []) }
    }

def build_lazada_payload(data):
    # Lazada Payload dibungkus object 'payload'
    total = sum(int(w.get('quantity', 0)) for w in data.get('warehouses', []))
    
    return {
        "payload": {
            "Attributes": {
                "name": data.get('name'),
                "short_description": data.get('description', '')
            },
            "Skus": [{
                "SellerSku": data.get('sku'),
                "quantity": total,
                "price": int(data.get('base_price', 0)),
                "Images": data.get('images', [])
            }]
        }
    }

# ==========================================
# 2. SYNC LOGIC (Service Bus Trigger)
# ==========================================
@app.service_bus_topic_trigger(
    arg_name="msg", 
    topic_name="product-events", 
    subscription_name="sync-service-sub", 
    connection="SERVICE_BUS_CONNECTION"
)
def process_sync_events(msg: func.ServiceBusMessage):
    event = json.loads(msg.get_body().decode("utf-8"))
    sku = event.get('sku')
    action = event.get('action')
    data = event.get('data', {})
    
    logging.info(f"[Sync] Processing {action} for {sku}")
    ctr = get_container()

    # --- LOGIKA 1: CREATE / UPDATE INFO PRODUK ---
    if action in ["PRODUCT_CREATED", "PRODUCT_UPDATED"]:
        channels = data.get('connected_channels', [])
        if not channels: return

        for marketplace in channels:
            try:
                binding_id = f"{marketplace}_{sku}"
                is_update = False
                ext_id = None
                
                try:
                    doc = ctr.read_item(item=binding_id, partition_key=sku)
                    is_update = True
                    ext_id = doc['external_id']
                except exceptions.CosmosResourceNotFoundError: pass

                # Tentukan URL & Payload (Sederhana)
                url, payload = "", {}
                if marketplace == "TOKOPEDIA":
                    payload = build_tokopedia_payload(data)
                    url = f"{MOCK_API_BASE_URL}/mock/tokopedia/product/202309/products" 
                    if is_update: url += f"/{ext_id}/inventory/update" # Mock logic terbatas
                
                elif marketplace == "SHOPEE":
                    payload = build_shopee_payload(data)
                    url = f"{MOCK_API_BASE_URL}/mock/shopee/api/v2/product/add_item"
                
                elif marketplace == "LAZADA":
                    payload = build_lazada_payload(data)
                    url = f"{MOCK_API_BASE_URL}/mock/lazada/product/create"

                # Eksekusi
                logging.info(f"   -> Sending {action} to {marketplace}")
                resp = requests.post(url, json=payload, timeout=10)
                
                # Simpan Binding (Jika Create)
                if not is_update and resp.status_code in [200, 201]:
                    # (Parsing ID Mock sederhana)
                    new_id = "MOCK-123" 
                    if marketplace == "TOKOPEDIA": new_id = str(resp.json()['data']['product_id'])
                    elif marketplace == "SHOPEE": new_id = str(resp.json()['response']['item_id'])
                    elif marketplace == "LAZADA": new_id = str(resp.json()['data']['item_id'])
                    
                    ctr.upsert_item({
                        "id": binding_id, "master_sku": sku, "marketplace": marketplace,
                        "external_id": new_id, "sync_status": "LINKED", "last_synced_at": get_iso_timestamp()
                    })

            except Exception as e: logging.error(f"Failed sync {marketplace}: {e}")

    # --- LOGIKA 2: UPDATE STOCK ONLY ---
    elif action == "STOCK_CHANGED":
        # Cari binding yang ada
        query = "SELECT * FROM c WHERE c.master_sku = @sku"
        bindings = list(ctr.query_items(query=query, parameters=[{"name":"@sku", "value":sku}]))
        
        for b in bindings:
            marketplace = b['marketplace']
            ext_id = b['external_id']
            try:
                url, payload = "", {}
                # Ambil Total Available dari payload Inventory Service
                total_qty = data.get('total_available', 0)
                
                if marketplace == "TOKOPEDIA":
                    # Tokopedia butuh array warehouse
                    inv_list = []
                    for w in data.get('warehouses', []):
                        inv_list.append({"warehouse_id": w['warehouse_code'], "quantity": w['quantity']})
                    url = f"{MOCK_API_BASE_URL}/mock/tokopedia/product/202309/products/{ext_id}/inventory/update"
                    payload = { "skus": [{ "inventory": inv_list }] }

                elif marketplace == "SHOPEE":
                    url = f"{MOCK_API_BASE_URL}/mock/shopee/api/v2/product/update_stock"
                    payload = { "item_id": int(ext_id), "stock_list": [{"seller_stock": [{"stock": total_qty}]}] }

                elif marketplace == "LAZADA":
                    url = f"{MOCK_API_BASE_URL}/mock/lazada/product/price_quantity/update"
                    payload = { "payload": { "Skus": [{ "SellerSku": sku, "Quantity": total_qty }] } }

                requests.post(url, json=payload, timeout=5)
                logging.info(f"   -> Pushed Stock {total_qty} to {marketplace}")
                
            except Exception as e: logging.error(f"Failed stock push {marketplace}: {e}")

# import azure.functions as func
# import logging
# import json
# import requests
# import os
# from azure.cosmos import CosmosClient, PartitionKey, exceptions

# app = func.FunctionApp()

# ENDPOINT = os.environ.get("COSMOS_ENDPOINT")
# KEY = os.environ.get("COSMOS_KEY")
# DATABASE = os.environ.get("COSMOS_DATABASE")
# CONTAINER = os.environ.get("COSMOS_CONTAINER")
# SHOPEE_URL = os.environ.get("SHOPEE_API_URL")

# client = CosmosClient(ENDPOINT, KEY)
# database = client.get_database_client(DATABASE)
# container = database.get_container_client(CONTAINER)

# # ---------------------------------------------------------
# # HTTP Trigger: Sync Product Data (Full Item Update)
# # ---------------------------------------------------------
# @app.route(route="sync_marketplace_item", auth_level=func.AuthLevel.ANONYMOUS)
# def sync_marketplace_item(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Menerima request sinkronisasi detail produk.')

#     try:
#         # 1. Terima Data dari Aplikasi Utama Anda
#         # Asumsi: Aplikasi Anda mengirim data yang sudah lengkap untuk dipetakan
#         req_body = req.get_json()
        
#         # Validasi sederhana
#         if not req_body.get('item_sku') and not req_body.get('id'):
#              return func.HttpResponse(
#                 json.dumps({"error": "SKU atau Item ID wajib ada"}),
#                 status_code=400,
#                 mimetype="application/json"
#             )

#         # 2. Proses Sinkronisasi ke Marketplace (Shopee)
#         shopee_response = update_shopee_item(req_body)

#         return func.HttpResponse(
#             json.dumps({
#                 "platform": "shopee",
#                 "status": "processed",
#                 "api_response": shopee_response
#             }),
#             status_code=200,
#             mimetype="application/json"
#         )

#     except ValueError:
#         return func.HttpResponse(
#              json.dumps({"error": "Invalid JSON format"}),
#              status_code=400,
#              mimetype="application/json"
#         )
#     except Exception as e:
#         logging.error(f"Error: {str(e)}")
#         return func.HttpResponse(
#              json.dumps({"error": str(e)}),
#              status_code=500,
#              mimetype="application/json"
#         )

# # ---------------------------------------------------------
# # Helper Function: Mapping ke Shopee API V2 Structure
# # ---------------------------------------------------------
# def update_shopee_item(data):
#     """
#     Memetakan data internal ke struktur JSON Shopee V2.
#     Parameter 'data' adalah dictionary dari aplikasi utama Anda.
#     """

#     # --- EKSEKUSI KE SHOPEE API ---
#     # CATATAN: Anda perlu menangani Sign Calculation & Access Token Shopee di sini
#     # URL Endpoint: /api/v2/product/update_item atau /api/v2/product/add_item
#     container.create_item(body=data)
    
#     return requests.post(SHOPEE_URL, json=data).json()
#     # return {"status": "success_mock", "message": "Payload generated successfully"}