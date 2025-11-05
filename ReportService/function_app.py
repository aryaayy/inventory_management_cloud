import azure.functions as func
import datetime
import json
import logging
import requests
import os
import json

app = func.FunctionApp()

# Ambil token dan path dari environment (local.settings.json)
TOKEN = os.getenv("AUTH_TOKEN")
PRODUCTS_DB_PATH = os.getenv("REPORT_DB_PATH")

def load_products():
    try:
        with open(PRODUCTS_DB_PATH, "r") as f:
            data = json.load(f)
        return data.get("products", []), None
    except Exception as e:
        return None, str(e)

def generate_product_report(products):
    if not products:
        return {"error": "No products found"}
    total_products = len(products)
    total_value = sum(p.get("price", 0) * p.get("stock", 1) for p in products)

    report_lines = [
        f"=== Scheduled Product Report ===",
        f"Generated at: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        f"Total products: {total_products}",
        f"Total inventory value: {total_value:,} IDR",
        "Top products:"
    ]

    for i, p in enumerate(products[:5], start=1):
        report_lines.append(f"{i}. {p.get('name')} - Price: {p.get('price'):,} IDR, Stock: {p.get('stock')}")

    if total_products > 5:
        report_lines.append(f"... and {total_products - 5} more products")

    return "\n".join(report_lines)

@app.schedule(schedule="0 */1 * * * *", arg_name="timer", run_on_startup=True, use_monitor=False)
def scheduled_report(timer: func.TimerRequest) -> None:
    try:
        products, err = load_products()
        if err:
            logging.error(f"Failed to load products: {err}")
            return
        report = generate_product_report(products)
        logging.info(report)
    except Exception as e:
        logging.error(f"Scheduled report error: {e}")

@app.route(route="report/run", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def run_report(req: func.HttpRequest) -> func.HttpResponse:
    token_header = req.headers.get("Authorization")
    if not token_header or token_header.replace("Bearer ", "") != TOKEN:
        return func.HttpResponse("Unauthorized", status_code=401)
    products, err = load_products()
    if err:
        return func.HttpResponse(f"Failed to load products: {err}", status_code=500)
    report = generate_product_report(products)
    return func.HttpResponse(json.dumps(report), mimetype="application/json", status_code=200)