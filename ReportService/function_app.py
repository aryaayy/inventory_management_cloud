import azure.functions as func
import datetime
import json
import logging
import requests
import os
import json, os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.auth import require_role, require_user, error

app = func.FunctionApp()

# Ambil token dan path dari environment (local.settings.json)
TOKEN = os.getenv("AUTH_TOKEN")
INVENTORIES_DB_PATH = os.getenv("REPORT_DB_PATH")

def load_inventories(req: func.HttpRequest):
    try:
        data = requests.get(
            "http://localhost:7073/api/inventory",
            headers={
                "Authorization": req.headers.get("Authorization")
            }
        )
        return data.json(), None
    except Exception as e:
        return None, str(e)
    
def load_inventories2():
    try:
        headers = {"Authorization": f"Bearer {TOKEN}"}
        response = requests.get(
            "http://localhost:7073/api/inventory",
            headers=headers
        )
        return response.json(), None
    except Exception as e:
        return None, str(e)

def generate_inventory_report(inventories):
    if not inventories:
        return {"error": "No inventories found"}
    total_inventories = len(inventories)
    total_value = sum(p["product"]["price"] * p["available_qty"] for p in inventories)

    report_lines = [
        f"=== Scheduled Inventory Report ===",
        f"Generated at: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        f"Total inventories: {total_inventories}",
        f"Total inventory value: {total_value:,} IDR",
        "Top products:"
    ]

    for i, p in enumerate(inventories[:5], start=1):
        report_lines.append(f"{i}. {p['product']['name']} - Price: {p['product']['price']:,} IDR, Available Stock: {p['available_qty']}, Sold: {p['sold_qty']}, In Process {p['reserved_qty']}")

    if total_inventories > 5:
        report_lines.append(f"... and {total_inventories - 5} more products")

    return "\n".join(report_lines)

@app.schedule(schedule="0 */1 * * * *", arg_name="timer", run_on_startup=True, use_monitor=False)
def scheduled_report(timer: func.TimerRequest) -> None:
    try:
        inventories, err = load_inventories2()
        if err:
            logging.error(f"Failed to load inventories: {err}")
            return
        report = generate_inventory_report(inventories)
        logging.info(report)
    except Exception as e:
        logging.error(f"Scheduled report error: {e}")

@app.route(route="report/run", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def run_report(req: func.HttpRequest) -> func.HttpResponse:
    claims, err = require_user(req)
    if err: 
        return err
    
    inventories, err = load_inventories(req)
    if err:
        return func.HttpResponse(f"Failed to load inventories: {err}", status_code=500)
    report = generate_inventory_report(inventories)
    return func.HttpResponse(json.dumps(report), mimetype="application/json", status_code=200)