import azure.functions as func
import logging
import json
import requests
import os
from azure.cosmos import CosmosClient, PartitionKey, exceptions

app = func.FunctionApp()

ENDPOINT = os.environ.get("COSMOS_ENDPOINT")
KEY = os.environ.get("COSMOS_KEY")
DATABASE = os.environ.get("COSMOS_DATABASE")
CONTAINER = os.environ.get("COSMOS_CONTAINER")
SHOPEE_URL = os.environ.get("SHOPEE_API_URL")

client = CosmosClient(ENDPOINT, KEY)
database = client.get_database_client(DATABASE)
container = database.get_container_client(CONTAINER)

# ---------------------------------------------------------
# HTTP Trigger: Sync Product Data (Full Item Update)
# ---------------------------------------------------------
@app.route(route="sync_marketplace_item", auth_level=func.AuthLevel.ANONYMOUS)
def sync_marketplace_item(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Menerima request sinkronisasi detail produk.')

    try:
        # 1. Terima Data dari Aplikasi Utama Anda
        # Asumsi: Aplikasi Anda mengirim data yang sudah lengkap untuk dipetakan
        req_body = req.get_json()
        
        # Validasi sederhana
        if not req_body.get('item_sku') and not req_body.get('id'):
             return func.HttpResponse(
                json.dumps({"error": "SKU atau Item ID wajib ada"}),
                status_code=400,
                mimetype="application/json"
            )

        # 2. Proses Sinkronisasi ke Marketplace (Shopee)
        shopee_response = update_shopee_item(req_body)

        return func.HttpResponse(
            json.dumps({
                "platform": "shopee",
                "status": "processed",
                "api_response": shopee_response
            }),
            status_code=200,
            mimetype="application/json"
        )

    except ValueError:
        return func.HttpResponse(
             json.dumps({"error": "Invalid JSON format"}),
             status_code=400,
             mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse(
             json.dumps({"error": str(e)}),
             status_code=500,
             mimetype="application/json"
        )

# ---------------------------------------------------------
# Helper Function: Mapping ke Shopee API V2 Structure
# ---------------------------------------------------------
def update_shopee_item(data):
    """
    Memetakan data internal ke struktur JSON Shopee V2.
    Parameter 'data' adalah dictionary dari aplikasi utama Anda.
    """

    # --- EKSEKUSI KE SHOPEE API ---
    # CATATAN: Anda perlu menangani Sign Calculation & Access Token Shopee di sini
    # URL Endpoint: /api/v2/product/update_item atau /api/v2/product/add_item
    container.create_item(body=data)
    
    return requests.post(SHOPEE_URL, json=data).json()
    # return {"status": "success_mock", "message": "Payload generated successfully"}