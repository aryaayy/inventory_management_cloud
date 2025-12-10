import azure.functions as func
from azure.cosmos import CosmosClient, exceptions, PartitionKey
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import logging
import json
import os
import uuid
import datetime

app = func.FunctionApp()

# --- CONFIGURATION ---
ENDPOINT = os.environ.get("COSMOS_ENDPOINT")
KEY = os.environ.get("COSMOS_KEY")
DATABASE_NAME = os.environ.get("COSMOS_DATABASE")
CONTAINER_INVENTORY = "inventory_items"
CONTAINER_LEDGER = "stock_ledger"       # <--- Container Baru untuk Riwayat
SB_CONN_STR = os.environ.get("SERVICE_BUS_CONNECTION")
TOPIC_NAME = "product-events"

client = None
db_client = None

def get_container(container_name):
    """Helper dinamis untuk mengambil container (Inventory atau Ledger)"""
    global client, db_client
    if not client:
        try:
            client = CosmosClient(ENDPOINT, KEY)
            db_client = client.get_database_client(DATABASE_NAME)
        except Exception as e:
            logging.error(f"DB Connection Error: {e}")
            raise e
            
    # Auto-create container jika belum ada (Biar tidak error 404)
    # Partition Key Ledger: /sku (Agar mudah tracking history per barang)
    pk_path = "/sku"
    return db_client.create_container_if_not_exists(id=container_name, partition_key=PartitionKey(path=pk_path))

def get_iso_timestamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def create_ledger_entry(sku, warehouse_code, change_qty, balance_after, reason, product_name="", price={}, ref_id=""):
    """
    Fungsi Pembukuan: Mencatat setiap perubahan stok ke tabel stock_ledger.
    """
    try:
        ledger_ctr = get_container(CONTAINER_LEDGER)
        
        ledger_item = {
            "id": str(uuid.uuid4()),        # ID Unik Transaksi
            "sku": sku,                     # Partition Key
            "warehouse_code": warehouse_code,
            "change_amount": change_qty,    # Misal: -5 atau +10
            "balance_after": balance_after, # Stok akhir setelah kejadian (Snapshot)
            "price": price,
            "reason": reason,               # ORDER_CREATED, ADJUSTMENT, dll
            "reference_id": ref_id,         # Order ID atau Note
            "timestamp": get_iso_timestamp()
        }
        
        ledger_ctr.create_item(body=ledger_item)
        logging.info(f"   [Ledger] Recorded: {sku} ({change_qty}) due to {reason}")
        
    except Exception as e:
        # PENTING: Jangan biarkan error ledger membatalkan transaksi utama!
        # Cukup log error-nya saja.
        logging.error(f"   [!] Failed to write ledger: {e}")

def publish_stock_event(sku, product_name):
    """Mengambil total stok Available dari semua gudang, lalu kirim ke Service Bus."""
    if not SB_CONN_STR: return
    
    ctr = get_container(CONTAINER_INVENTORY)
    
    # Aggregate Stok
    query = "SELECT * FROM c WHERE c.sku = @sku"
    params = [{"name": "@sku", "value": sku}]
    items = list(ctr.query_items(query=query, parameters=params, enable_cross_partition_query=True))
    
    if not items: return

    warehouses_list = []
    total_avail = 0
    for item in items:
        qty = item.get('quantity_available', 0)
        total_avail += qty
        warehouses_list.append({
            "warehouse_code": item['warehouse_code'],
            "quantity": qty
        })
    
    payload = {
        "action": "STOCK_CHANGED",
        "sku": sku,
        "data": {
            "name": product_name or items[0].get('product_name'),
            "warehouses": warehouses_list,
            "total_available": total_avail,
            "connected_channels": [] 
        }
    }

    try:
        client = ServiceBusClient.from_connection_string(SB_CONN_STR)
        with client:
            sender = client.get_topic_sender(TOPIC_NAME)
            with sender:
                msg = ServiceBusMessage(json.dumps(payload))
                sender.send_messages(msg)
        logging.info(f"[Inventory] Published STOCK_CHANGED for {sku}")
    except Exception as e:
        logging.error(f"[Inventory] Failed to publish stock event: {e}")

# ==========================================
# 1. ADJUST INVENTORY (Manual Stock Opname)
# ==========================================
@app.route(route="inventory/adjust", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def adjust_inventory(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Inventory Adjustment.')
    try:
        req_body = req.get_json()
        ctr = get_container(CONTAINER_INVENTORY)
        
        sku = req_body['sku']
        warehouse_code = req_body['warehouse_code']
        new_on_hand = int(req_body['quantity_on_hand'])
        safety_stock = int(req_body.get('safety_stock', 0))
        
        doc_id = f"{sku}_{warehouse_code}"

        # 1. Ambil data lama
        try:
            existing_item = ctr.read_item(item=doc_id, partition_key=sku)
            current_reserved = existing_item.get('quantity_reserved', 0)
            old_on_hand = existing_item.get('quantity_on_hand', 0)
            product_name = existing_item.get('product_name')
        except exceptions.CosmosResourceNotFoundError:
            current_reserved = 0
            old_on_hand = 0
            product_name = req_body.get('product_name', 'Unknown Product')

        # Hitung Selisih untuk Ledger (Baru - Lama)
        diff_qty = new_on_hand - old_on_hand

        # 2. Hitung Available
        available = new_on_hand - current_reserved

        # 3. Simpan Inventory Item
        inventory_item = {
            "id": doc_id,
            "sku": sku,
            "warehouse_code": warehouse_code,
            "quantity_on_hand": new_on_hand,
            "quantity_reserved": current_reserved,
            "quantity_available": available,
            "safety_stock": safety_stock,
            "product_name": product_name,
            "last_updated": get_iso_timestamp()
        }
        ctr.upsert_item(body=inventory_item)
        
        # 4. CATAT KE LEDGER (Jika ada perubahan)
        if diff_qty != 0:
            create_ledger_entry(
                sku=sku, 
                warehouse_code=warehouse_code, 
                change_qty=diff_qty, 
                balance_after=new_on_hand, # Balance ledger biasanya mengacu ke On Hand (Fisik)
                reason="MANUAL_ADJUSTMENT",
                ref_id="Opname-API"
            )
        
        # 5. Trigger Sync
        publish_stock_event(sku, product_name)
        
        return func.HttpResponse(json.dumps(inventory_item), mimetype="application/json", status_code=200)

    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

# ==========================================
# 2. LISTEN TO ORDER EVENTS (Checkout/Cancel)
# ==========================================
@app.service_bus_topic_trigger(
    arg_name="msg", 
    topic_name="marketplace-orders",
    subscription_name="inventory-order-sub",
    connection="SERVICE_BUS_CONNECTION"
)
def process_marketplace_orders(msg: func.ServiceBusMessage):
    message_body = msg.get_body().decode("utf-8")
    event = json.loads(message_body)
    
    action = event.get('action')
    sku = event.get('sku')
    qty = int(event.get('quantity', 0))
    product_name = event.get('product_name', '')
    price = event.get('price', {})
    wh_id = event.get('warehouse_code')
    order_id = event.get('order_id')

    logging.info(f"[Inventory] Processing Order {order_id}: {action} ({qty} pcs)")
    
    ctr = get_container(CONTAINER_INVENTORY)
    doc_id = f"{sku}_{wh_id}"

    try:
        item = ctr.read_item(item=doc_id, partition_key=sku)
        
        current_on_hand = item.get('quantity_on_hand', 0)
        current_reserved = item.get('quantity_reserved', 0)
        
        ledger_change = 0
        ledger_reason = action
        ledger_balance_ref = current_on_hand # Defaultnya kita track On Hand

        if action == "ORDER_CREATED":
            # Booking: On Hand tetap, Reserved nambah
            item['quantity_reserved'] = current_reserved + qty
            
            # Di Ledger, kita bisa catat "Booking" atau "Pending Sales"
            # Tapi fisik belum berubah, jadi change_qty = 0 atau kita anggap ini hold
            # Untuk simplicity, kita catat RESERVATION di ledger
            ledger_change = 0 
            ledger_reason = "ORDER_RESERVED"

        elif action == "ORDER_CANCELLED":
            # Batal: Reserved berkurang
            item['quantity_reserved'] = max(0, current_reserved - qty)
            ledger_change = 0
            ledger_reason = "ORDER_CANCELLED_RESTORE"

        elif action == "ORDER_COMPLETED":
            # Shipped: Fisik Keluar
            new_on_hand = max(0, current_on_hand - qty)
            item['quantity_on_hand'] = new_on_hand
            item['quantity_reserved'] = max(0, current_reserved - qty)
            
            # Ini yang masuk buku besar sebagai pengeluaran barang
            ledger_change = -qty 
            ledger_balance_ref = new_on_hand
            ledger_reason = "ORDER_FULFILLED"

        # Hitung Available & Simpan
        item['quantity_available'] = item['quantity_on_hand'] - item['quantity_reserved']
        item['last_updated'] = get_iso_timestamp()
        
        ctr.replace_item(item=doc_id, body=item)
        
        # --- CATAT KE LEDGER ---
        # Kita catat setiap event order agar history lengkap
        # Walaupun change=0 (saat reserved), tetap dicatat agar tahu ada order masuk
        create_ledger_entry(
            sku=sku,
            warehouse_code=wh_id,
            change_qty=ledger_change if action == "ORDER_COMPLETED" else qty, # Saat reserve, kita catat qty ordernya sebagai info
            balance_after=ledger_balance_ref,
            product_name=product_name,
            price=price,
            reason=ledger_reason,
            ref_id=order_id
        )

        publish_stock_event(sku, item.get('product_name'))

    except exceptions.CosmosResourceNotFoundError:
        logging.error(f"Inventory not found for {sku} in {wh_id}")
    except Exception as e:
        logging.error(f"Failed to process order {order_id}: {e}")

# ==========================================
# 3. LISTEN TO PRODUCT EVENTS (Init Inventory)
# ==========================================
@app.service_bus_topic_trigger(
    arg_name="msg", topic_name="product-events", subscription_name="inventory-service-sub", connection="SERVICE_BUS_CONNECTION"
)
def process_product_events_inventory(msg: func.ServiceBusMessage):
    event = json.loads(msg.get_body().decode("utf-8"))
    
    if event.get('action') == "PRODUCT_CREATED":
        sku = event.get('sku')
        warehouses = event.get('data', {}).get('warehouses', [])
        ctr = get_container(CONTAINER_INVENTORY)
        
        for wh in warehouses:
            try:
                wh_code = wh.get('warehouse_code')
                qty = int(wh.get('quantity', 0))
                doc_id = f"{sku}_{wh_code}"
                
                init_item = {
                    "id": doc_id, "sku": sku, "warehouse_code": wh_code,
                    "quantity_on_hand": qty, "quantity_reserved": 0, "quantity_available": qty,
                    "product_name": event.get('data', {}).get('name'), "last_updated": get_iso_timestamp()
                }
                ctr.upsert_item(body=init_item)
                
                # Catat Initial Stock ke Ledger
                create_ledger_entry(sku, wh_code, qty, qty, "INITIAL_STOCK", "Product-Create-Event")
                
                logging.info(f"Created Inventory {doc_id}")
            except: pass

# ==========================================
# 4. GET ORDER LIST (Collapsed State from Ledger)
# ==========================================
@app.route(route="inventory/orders", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_order_list(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Get Order List.')
    
    # Limit row ledger yang mau discan (Misal scan 100 kejadian terakhir)
    limit = int(req.params.get('scan_limit', '100')) 

    try:
        ctr = get_container(CONTAINER_LEDGER)
        
        # 1. Ambil Data yang berhubungan dengan Order saja (Abaikan Manual Adjustment)
        # Urutkan DESC (Terbaru di atas)
        query = """
            SELECT * FROM c 
            WHERE c.reason IN ('ORDER_RESERVED', 'ORDER_FULFILLED', 'ORDER_CANCELLED_RESTORE')
            ORDER BY c.timestamp DESC 
            OFFSET 0 LIMIT @limit
        """
        
        items = list(ctr.query_items(
            query=query,
            parameters=[{"name": "@limit", "value": limit}],
            enable_cross_partition_query=True
        ))

        # 2. Proses Deduplikasi (Ambil status terakhir per Order ID)
        unique_orders = {}
        
        for item in items:
            order_id = item.get('reference_id')
            
            # Jika Order ID ini belum ada di dictionary, berarti ini status terbarunya
            if order_id and order_id not in unique_orders:
                
                # Mapping Status Teknis ke Bahasa Manusia
                raw_reason = item.get('reason')
                status_display = "PENDING" # Default (ORDER_RESERVED)
                
                if raw_reason == "ORDER_FULFILLED":
                    status_display = "COMPLETED"
                elif raw_reason == "ORDER_CANCELLED_RESTORE":
                    status_display = "CANCELLED"
                
                unique_orders[order_id] = {
                    "order_id": order_id,
                    "latest_status": status_display,
                    "last_updated": item.get('timestamp'),
                    "product_name": item.get('product_name'),
                    "sku": item.get('sku'),
                    "price": item.get('unit_price'),
                    "quantity": abs(item.get('change_amount', 0)) or "N/A" 
                    # Note: qty mungkin 0 kalau statusnya Cancel/Reserved, 
                    # jadi logic qty ini bisa disesuaikan kebutuhan
                }

        # 3. Ubah Dictionary kembali ke List
        final_list = list(unique_orders.values())

        return func.HttpResponse(json.dumps(final_list), mimetype="application/json", status_code=200)

    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

# import azure.functions as func
# from azure.cosmos import CosmosClient, exceptions
# from azure.servicebus import ServiceBusClient, ServiceBusMessage
# import logging
# import json
# import os
# import datetime

# app = func.FunctionApp()

# # --- CONFIGURATION ---
# ENDPOINT = os.environ.get("COSMOS_ENDPOINT")
# KEY = os.environ.get("COSMOS_KEY")
# DATABASE_NAME = os.environ.get("COSMOS_DATABASE")
# CONTAINER_NAME = "inventory_items" # Hardcoded agar tidak salah
# SB_CONN_STR = os.environ.get("SERVICE_BUS_CONNECTION")
# TOPIC_NAME = "product-events"

# client = None
# container = None

# def get_container():
#     global client, container
#     if not container:
#         client = CosmosClient(ENDPOINT, KEY)
#         database = client.get_database_client(DATABASE_NAME)
#         container = database.get_container_client(CONTAINER_NAME)
#     return container

# def get_iso_timestamp():
#     return datetime.datetime.now(datetime.timezone.utc).isoformat()

# def publish_stock_event(sku, product_name):
#     """Mengambil total stok Available dari semua gudang, lalu kirim ke Service Bus."""
#     if not SB_CONN_STR: return
    
#     ctr = get_container()
    
#     # 1. Aggregate Stok dari Semua Gudang
#     query = "SELECT * FROM c WHERE c.sku = @sku"
#     params = [{"name": "@sku", "value": sku}]
#     items = list(ctr.query_items(query=query, parameters=params, enable_cross_partition_query=True))
    
#     if not items: return

#     warehouses_list = []
#     total_avail = 0
#     for item in items:
#         qty = item.get('quantity_available', 0)
#         total_avail += qty
#         warehouses_list.append({
#             "warehouse_code": item['warehouse_code'],
#             "quantity": qty
#         })
    
#     # 2. Kirim Event STOCK_CHANGED
#     payload = {
#         "action": "STOCK_CHANGED",
#         "sku": sku,
#         "data": {
#             "name": product_name or items[0].get('product_name'),
#             "warehouses": warehouses_list,
#             "total_available": total_avail,
#             "connected_channels": [] # Sync Service akan mencari binding sendiri
#         }
#     }

#     try:
#         client = ServiceBusClient.from_connection_string(SB_CONN_STR)
#         with client:
#             sender = client.get_topic_sender(TOPIC_NAME)
#             with sender:
#                 msg = ServiceBusMessage(json.dumps(payload))
#                 sender.send_messages(msg)
#         logging.info(f"[Inventory] Published STOCK_CHANGED for {sku}")
#     except Exception as e:
#         logging.error(f"[Inventory] Failed to publish stock event: {e}")

# # ==========================================
# # 1. ADJUST INVENTORY (Manual Stock Opname)
# # ==========================================
# @app.route(route="inventory/adjust", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
# def adjust_inventory(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Processing Inventory Adjustment.')
#     try:
#         req_body = req.get_json()
#         ctr = get_container()
        
#         sku = req_body['sku']
#         warehouse_code = req_body['warehouse_code']
#         new_on_hand = int(req_body['quantity_on_hand'])
#         safety_stock = int(req_body.get('safety_stock', 0))
        
#         doc_id = f"{sku}_{warehouse_code}"

#         # 1. Ambil data lama untuk Reserved
#         try:
#             existing_item = ctr.read_item(item=doc_id, partition_key=sku)
#             current_reserved = existing_item.get('quantity_reserved', 0)
#             product_name = existing_item.get('product_name')
#         except exceptions.CosmosResourceNotFoundError:
#             current_reserved = 0
#             product_name = req_body.get('product_name', 'Unknown Product')

#         # 2. Hitung Available
#         available = new_on_hand - current_reserved

#         # 3. Simpan
#         inventory_item = {
#             "id": doc_id,
#             "sku": sku,
#             "warehouse_code": warehouse_code,
#             "quantity_on_hand": new_on_hand,
#             "quantity_reserved": current_reserved,
#             "quantity_available": available,
#             "safety_stock": safety_stock,
#             "product_name": product_name,
#             "last_updated": get_iso_timestamp()
#         }
#         ctr.upsert_item(body=inventory_item)
        
#         # 4. Trigger Sync
#         publish_stock_event(sku, product_name)
        
#         return func.HttpResponse(json.dumps(inventory_item), mimetype="application/json", status_code=200)

#     except Exception as e:
#         return func.HttpResponse(f"Error: {str(e)}", status_code=500)

# # ==========================================
# # 2. LISTEN TO ORDER EVENTS (Checkout/Cancel)
# # ==========================================
# @app.service_bus_topic_trigger(
#     arg_name="msg", 
#     topic_name="marketplace-orders",
#     subscription_name="inventory-order-sub",
#     connection="SERVICE_BUS_CONNECTION"
# )
# def process_marketplace_orders(msg: func.ServiceBusMessage):
#     message_body = msg.get_body().decode("utf-8")
#     event = json.loads(message_body)
    
#     action = event.get('action')
#     sku = event.get('sku')
#     qty = int(event.get('quantity', 0))
#     wh_id = event.get('warehouse_code')
#     order_id = event.get('order_id')

#     logging.info(f"[Inventory] Processing Order {order_id}: {action} ({qty} pcs)")
#     ctr = get_container()
#     doc_id = f"{sku}_{wh_id}"

#     try:
#         item = ctr.read_item(item=doc_id, partition_key=sku)
        
#         current_on_hand = item.get('quantity_on_hand', 0)
#         current_reserved = item.get('quantity_reserved', 0)

#         if action == "ORDER_CREATED":
#             item['quantity_reserved'] = current_reserved + qty
            
#         elif action == "ORDER_CANCELLED":
#             item['quantity_reserved'] = max(0, current_reserved - qty)
            
#         elif action == "ORDER_COMPLETED":
#             item['quantity_on_hand'] = max(0, current_on_hand - qty)
#             item['quantity_reserved'] = max(0, current_reserved - qty)

#         # Hitung Ulang Available
#         item['quantity_available'] = item['quantity_on_hand'] - item['quantity_reserved']
#         item['last_updated'] = get_iso_timestamp()

#         ctr.replace_item(item=doc_id, body=item)
        
#         # PENTING: Trigger Update ke Marketplace lain!
#         publish_stock_event(sku, item.get('product_name'))

#     except exceptions.CosmosResourceNotFoundError:
#         logging.error(f"Inventory not found for {sku} in {wh_id}")
#     except Exception as e:
#         logging.error(f"Failed to process order {order_id}: {e}")

# # ==========================================
# # 3. LISTEN TO PRODUCT EVENTS (Init Inventory)
# # ==========================================
# @app.service_bus_topic_trigger(
#     arg_name="msg", topic_name="product-events", subscription_name="inventory-service-sub", connection="SERVICE_BUS_CONNECTION"
# )
# def process_product_events_inventory(msg: func.ServiceBusMessage):
#     event = json.loads(msg.get_body().decode("utf-8"))
#     if event.get('action') == "PRODUCT_CREATED":
#         sku = event.get('sku')
#         warehouses = event.get('data', {}).get('warehouses', [])
#         ctr = get_container()
        
#         for wh in warehouses:
#             try:
#                 wh_code = wh.get('warehouse_code')
#                 qty = int(wh.get('quantity', 0))
#                 doc_id = f"{sku}_{wh_code}"
                
#                 init_item = {
#                     "id": doc_id, "sku": sku, "warehouse_code": wh_code,
#                     "quantity_on_hand": qty, "quantity_reserved": 0, "quantity_available": qty,
#                     "product_name": event.get('data', {}).get('name'), "last_updated": get_iso_timestamp()
#                 }
#                 ctr.upsert_item(body=init_item)
#                 logging.info(f"Created Inventory {doc_id}")
#             except: pass
# import azure.functions as func
# from azure.cosmos import CosmosClient, exceptions
# import logging
# import json
# import os
# import datetime
# from azure.servicebus import ServiceBusClient, ServiceBusMessage

# app = func.FunctionApp()

# ENDPOINT = os.environ.get("COSMOS_ENDPOINT")
# KEY = os.environ.get("COSMOS_KEY")
# DATABASE_NAME = os.environ.get("COSMOS_DATABASE")
# CONTAINER_NAME = os.environ.get("COSMOS_CONTAINER") # inventory_items
# SB_CONN_STR = os.environ.get("SERVICE_BUS_CONNECTION")
# TOPIC_NAME = "product-events"

# client = None
# container = None

# def get_container():
#     global client, container
#     if not container:
#         try:
#             client = CosmosClient(ENDPOINT, KEY)
#             database = client.get_database_client(DATABASE_NAME)
#             container = database.get_container_client(CONTAINER_NAME)
#         except Exception as e:
#             logging.error(f"Error connecting to Cosmos DB: {e}")
#             raise e
#     return container

# def get_iso_timestamp():
#     return datetime.datetime.now(datetime.timezone.utc).isoformat()

# def publish_stock_event(sku, product_name):
#     """
#     Helper: Mengambil total stok terbaru dari semua gudang, lalu kirim ke Bus.
#     """
#     if not SB_CONN_STR: return
    
#     ctr = get_container()
    
#     # 1. Ambil semua data gudang untuk SKU ini (Aggregate)
#     query = "SELECT * FROM c WHERE c.sku = @sku"
#     params = [{"name": "@sku", "value": sku}]
#     items = list(ctr.query_items(query=query, parameters=params, enable_cross_partition_query=True))
    
#     if not items: return

#     # 2. Susun Payload "Warehouses"
#     warehouses_list = []
#     for item in items:
#         warehouses_list.append({
#             "warehouse_code": item['warehouse_code'],
#             "quantity": item['quantity_available'] # Kirim Available, BUKAN On Hand
#         })
    
#     # 3. Kirim Event STOCK_CHANGED
#     payload = {
#         "action": "STOCK_CHANGED",
#         "sku": sku,
#         "data": {
#             "name": product_name or items[0].get('product_name'),
#             "warehouses": warehouses_list,
#             # Kita kirim connected_channels kosong atau biarkan Sync Service cari sendiri via Binding
#             "connected_channels": [] 
#         }
#     }

#     try:
#         client = ServiceBusClient.from_connection_string(SB_CONN_STR)
#         with client:
#             sender = client.get_topic_sender(TOPIC_NAME)
#             with sender:
#                 msg = ServiceBusMessage(json.dumps(payload))
#                 sender.send_messages(msg)
#         logging.info(f"[Inventory] Published STOCK_CHANGED for {sku}")
#     except Exception as e:
#         logging.error(f"[Inventory] Failed to publish stock event: {e}")

# # ==========================================
# # 1. ADJUST INVENTORY (Create/Update per Warehouse)
# # ==========================================
# @app.route(route="inventory/adjust", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
# def adjust_inventory(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Processing Inventory Adjustment.')
#     try:
#         req_body = req.get_json()
#         ctr = get_container()
#     except ValueError:
#         return func.HttpResponse("Invalid JSON", status_code=400)

#     # Validasi Input
#     if not all(k in req_body for k in ('sku', 'warehouse_code', 'quantity_on_hand')):
#         return func.HttpResponse("Need 'sku', 'warehouse_code', and 'quantity_on_hand'", status_code=400)

#     sku = req_body['sku']
#     warehouse_code = req_body['warehouse_code']
#     new_on_hand = int(req_body['quantity_on_hand'])
#     safety_stock = int(req_body.get('safety_stock', 0))

#     # ID Unik = SKU + Warehouse (agar satu produk bisa ada di banyak gudang)
#     doc_id = f"{sku}_{warehouse_code}"

#     try:
#         # 1. Coba ambil data lama (untuk mempertahankan quantity_reserved)
#         try:
#             # Partition Key = sku, ID = doc_id
#             existing_item = ctr.read_item(item=doc_id, partition_key=sku)
#             current_reserved = existing_item.get('quantity_reserved', 0)
#         except exceptions.CosmosResourceNotFoundError:
#             # Jika belum ada, reserved = 0
#             current_reserved = 0

#         # 2. Hitung Available (On Hand - Reserved)
#         available = new_on_hand - current_reserved

#         # 3. Struktur Data Baru
#         inventory_item = {
#             "id": doc_id,              # Composite ID
#             "sku": sku,                # Partition Key
#             "warehouse_code": warehouse_code,
#             "quantity_on_hand": new_on_hand,
#             "quantity_reserved": current_reserved,
#             "quantity_available": available,
#             "safety_stock": safety_stock,
#             "last_updated": get_iso_timestamp()
#         }

#         # 4. Simpan
#         ctr.upsert_item(body=inventory_item)
        
#         return func.HttpResponse(json.dumps(inventory_item), mimetype="application/json", status_code=200)

#     except exceptions.CosmosHttpResponseError as e:
#         return func.HttpResponse(f"DB Error: {str(e)}", status_code=500)


# # ==========================================
# # 2. GET INVENTORY (Read All Warehouses for SKU)
# # ==========================================
# @app.route(route="inventory/get", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
# def get_inventory(req: func.HttpRequest) -> func.HttpResponse:
#     sku = req.params.get('sku')
#     if not sku:
#         return func.HttpResponse("Parameter 'sku' is required", status_code=400)

#     ctr = get_container()
#     try:
#         # Query: Ambil semua dokumen dengan SKU tersebut (bisa > 1 gudang)
#         query = "SELECT * FROM c WHERE c.sku = @sku"
#         params = [{"name": "@sku", "value": sku}]
        
#         items = list(ctr.query_items(query=query, parameters=params))
        
#         if not items:
#             return func.HttpResponse("Inventory data not found", status_code=404)

#         return func.HttpResponse(json.dumps(items), mimetype="application/json", status_code=200)

#     except exceptions.CosmosHttpResponseError as e:
#         return func.HttpResponse(f"DB Error: {str(e)}", status_code=500)


# # ==========================================
# # 3. DELETE INVENTORY (Delete All Warehouses)
# # ==========================================
# @app.route(route="inventory/delete", methods=["DELETE"], auth_level=func.AuthLevel.ANONYMOUS)
# def delete_inventory(req: func.HttpRequest) -> func.HttpResponse:
#     sku = req.params.get('sku')
#     if not sku:
#         return func.HttpResponse("Parameter 'sku' is required", status_code=400)

#     ctr = get_container()
#     try:
#         # 1. Cari dulu semua item dengan SKU ini
#         query = "SELECT c.id, c.sku FROM c WHERE c.sku = @sku"
#         params = [{"name": "@sku", "value": sku}]
#         items = list(ctr.query_items(query=query, parameters=params))

#         if not items:
#              return func.HttpResponse("Inventory not found", status_code=404)

#         # 2. Hapus satu per satu
#         for item in items:
#             ctr.delete_item(item=item['id'], partition_key=item['sku'])

#         return func.HttpResponse(f"Deleted {len(items)} inventory records for {sku}", status_code=200)

#     except exceptions.CosmosHttpResponseError as e:
#         return func.HttpResponse(f"DB Error: {str(e)}", status_code=500)


# # ==========================================
# # 4. LISTEN TO PRODUCT EVENTS
# # ==========================================
# @app.service_bus_topic_trigger(
#     arg_name="msg", 
#     topic_name="product-events", 
#     subscription_name="inventory-service-sub",
#     connection="SERVICE_BUS_CONNECTION"
# )
# def process_product_events_inventory(msg: func.ServiceBusMessage):
#     message_body = msg.get_body().decode("utf-8")
#     event = json.loads(message_body)
    
#     sku = event.get('sku')
#     action = event.get('action')
#     product_data = event.get('data', {}) # Data lengkap dari Product Service
    
#     logging.info(f"[Inventory] Received event {action} for {sku}")
#     ctr = get_container()

#     if action == "PRODUCT_CREATED":
#         # Ambil list warehouses dari data product
#         warehouses_from_product = product_data.get('warehouses', [])
        
#         if not warehouses_from_product:
#             logging.info(f"No initial warehouses defined for {sku}. Skipping auto-create.")
#             return

#         logging.info(f"Auto-creating inventory for {len(warehouses_from_product)} warehouses...")

#         # LOOPING: Buat inventory record untuk SETIAP gudang
#         for wh in warehouses_from_product:
#             try:
#                 wh_code = wh.get('warehouse_code')
#                 qty = int(wh.get('quantity', 0))
                
#                 # Composite ID
#                 doc_id = f"{sku}_{wh_code}"

#                 init_inventory = {
#                     "id": doc_id,              # ID Unik
#                     "sku": sku,                # Partition Key
#                     "warehouse_code": wh_code,
#                     "quantity_on_hand": qty,
#                     "quantity_reserved": 0,
#                     "quantity_available": qty, # Awal create, available = on_hand
#                     "safety_stock": 0,
#                     "last_updated": get_iso_timestamp(),
#                     # Cache info produk biar gampang dibaca di DB Inventory
#                     "product_name": product_data.get('name') 
#                 }
                
#                 # Gunakan Create (bukan Upsert) agar aman, atau Upsert jika ingin overwrite
#                 ctr.upsert_item(body=init_inventory)
#                 logging.info(f"   --> Created inventory: {doc_id} (Qty: {qty})")
                
#             except Exception as e:
#                 logging.error(f"Failed to create inventory item for {sku} in {wh}: {e}")

#     elif action == "PRODUCT_DELETED":
#         # ... (Logika delete tetap sama: Hapus semua berdasarkan SKU) ...
#         try:
#             query = "SELECT c.id, c.sku FROM c WHERE c.sku = @sku"
#             params = [{"name": "@sku", "value": sku}]
#             items = list(ctr.query_items(query=query, parameters=params))
#             for item in items:
#                 ctr.delete_item(item=item['id'], partition_key=item['sku'])
#             logging.info(f"Deleted inventory records for {sku}")
#         except Exception as e:
#             logging.error(f"Failed delete cleanup: {e}")

# @app.service_bus_topic_trigger(
#     arg_name="msg", 
#     topic_name="marketplace-orders",
#     subscription_name="inventory-order-sub",
#     connection="SERVICE_BUS_CONNECTION"
# )
# def process_marketplace_orders(msg: func.ServiceBusMessage):
#     message_body = msg.get_body().decode("utf-8")
#     event = json.loads(message_body)
    
#     action = event.get('action')
#     sku = event.get('sku')
#     qty = int(event.get('quantity', 0))
#     wh_id = event.get('warehouse_code')
#     order_id = event.get('order_id')

#     logging.info(f"[Inventory] Processing Order {order_id}: {action} ({qty} pcs)")

#     ctr = get_container()
#     doc_id = f"{sku}_{wh_id}"

#     try:
#         # 1. Ambil Data
#         item = ctr.read_item(item=doc_id, partition_key=sku)
        
#         current_on_hand = item.get('quantity_on_hand', 0)
#         current_reserved = item.get('quantity_reserved', 0)

#         # 2. Logika Perubahan Stok
#         if action == "ORDER_CREATED":
#             # Booking Stok
#             item['quantity_reserved'] = current_reserved + qty
#             logging.info(f"   -> Reserved increased. Booking stok.")

#         elif action == "ORDER_CANCELLED":
#             # Batal Booking (Balikin ke rak)
#             item['quantity_reserved'] = max(0, current_reserved - qty)
#             logging.info(f"   -> Reserved released. Stok kembali available.")

#         elif action == "ORDER_COMPLETED":
#             # Barang Terkirim (Fisik Keluar Gudang)
#             # Kurangi Fisik DAN Kurangi Reserved secara bersamaan
            
#             new_on_hand = current_on_hand - qty
#             new_reserved = current_reserved - qty
            
#             # Validasi agar tidak minus (Safety)
#             item['quantity_on_hand'] = max(0, new_on_hand)
#             item['quantity_reserved'] = max(0, new_reserved)
            
#             logging.info(f"   -> Order Shipped. On Hand decreased to {item['quantity_on_hand']}")

#         # 3. Hitung Ulang Available (Selalu Konsisten)
#         # Available = Fisik - Reserved
#         item['quantity_available'] = item['quantity_on_hand'] - item['quantity_reserved']
#         item['last_updated'] = get_iso_timestamp()

#         # 4. Simpan
#         ctr.replace_item(item=doc_id, body=item)

#     except exceptions.CosmosResourceNotFoundError:
#         logging.error(f"Inventory not found for {sku} in {wh_id}")
#     except Exception as e:
#         logging.error(f"Failed to process order {order_id}: {e}")

# # import azure.functions as func
# # import datetime
# # import json
# # import logging
# # import requests
# # import os, sys
# # sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# # from utils.auth import require_role, require_user, error

# # app = func.FunctionApp()

# # DATA_FILE = "inventories_db.json"

# # def _ensure_file():
# #     if not os.path.exists(DATA_FILE):
# #         with open(DATA_FILE, "w") as f:
# #             json.dump({"products": []}, f)

# # def _load():
# #     _ensure_file()
# #     with open(DATA_FILE, "r") as f:
# #         return json.load(f)

# # def _save(data):
# #     with open(DATA_FILE, "w") as f:
# #         json.dump(data, f, indent=4)

# # @app.route(route="inventory", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
# # def get_inventory(req: func.HttpRequest) -> func.HttpResponse:
# #     claims, err = require_user(req)
# #     if err:
# #         return err
    
# #     if not require_role(claims, ["Owner"]):
# #         return error("forbidden: owner only", 403)
    
# #     tenant = claims["tenantId"]
# #     data = _load()

# #     tenant_inventory = [
# #         p for p in data["inventories"] 
# #         if p.get("tenantId") in (None, tenant)
# #     ]

# #     try:
# #         products = requests.get(
# #             "http://localhost:7072/api/product/products",
# #             headers={
# #                 "Authorization": req.headers.get("Authorization"),
# #             }
# #         ).json()
# #     except Exception as e:
# #         return func.HttpResponse(e)

# #     product_map = {p["product_id"]: p for p in products}

# #     for row in tenant_inventory:
# #         product = product_map.get(row["product_id"])
# #         row["product"] = product
    
# #     return func.HttpResponse(json.dumps(tenant_inventory), status_code=200, mimetype="application/json")

# # @app.route(route="inventory/create", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
# # def create_inventory(req: func.HttpRequest) -> func.HttpResponse:
# #     claims, err = require_user(req)
# #     if err:
# #         return err
    
# #     if not require_role(claims, ["Owner"]):
# #         return error("forbidden: owner only", 403)
    
# #     try:
# #         body = req.get_json()
# #     except Exception as e:
# #         return func.HttpResponse(f"Bad Request: {e}", status_code=400)

# #     data = _load()

# #     prev_inventory_id = int(data['inventories'][-1]['inventory_id'].lstrip("I"))
# #     next_inventory_id = f"I{(prev_inventory_id + 1):03d}"
# #     new_inventory_item = {
# #         "inventory_id": next_inventory_id,
# #         "product_id": body['product_id'],
# #         "available_qty": body['available_qty'],
# #         "sold_qty": 0,
# #         "reserved_qty": 0,
# #         "tenantId": claims['tenantId']
# #     }

# #     data['inventories'].append(new_inventory_item)
# #     _save(data)
    
# #     return func.HttpResponse(json.dumps(new_inventory_item), status_code=200, mimetype="application/json")

# # @app.route(route="inventory/update", methods=["PUT"], auth_level=func.AuthLevel.FUNCTION)
# # def update_inventory(req: func.HttpRequest) -> func.HttpResponse:
# #     claims, err = require_user(req)
# #     if err:
# #         return err
    
# #     if not require_role(claims, ["Owner"]):
# #         return error("forbidden: owner only", 403)
    
# #     try:
# #         body = req.get_json()
# #         body['tenantId'] = claims['tenantId']
# #     except Exception as e:
# #         return func.HttpResponse(f"Bad Request: {e}", status_code=400)

# #     data = _load()
# #     i = 0
# #     for row in data['inventories']:
# #         if row["inventory_id"] == body['inventory_id'] and row["tenantId"] == claims["tenantId"]:
# #             data['inventories'][i] = body
# #             _save(data)
# #             return func.HttpResponse(json.dumps(body), status_code=200, mimetype="application/json")

# #         i += 1
    
# #     return error("not found", 404)

# # @app.route(route="inventory/delete", methods=["DELETE"], auth_level=func.AuthLevel.FUNCTION)
# # def delete_inventory(req: func.HttpRequest) -> func.HttpResponse:
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
# #     before = len(data["inventories"])
# #     data["inventories"] = [
# #         p for p in data["inventories"]
# #         if not (p["inventory_id"] == body["inventory_id"] and p["tenantId"] == claims["tenantId"])
# #     ]
# #     _save(data)

# #     if len(data["inventories"]) == before:
# #         return error("NOT FOUND", 404)

# #     return func.HttpResponse(f"Deleted {body['inventory_id']}", mimetype="text/plain")