import azure.functions as func
import datetime
import json
import logging
import requests

app = func.FunctionApp()

@app.route(route="MyHttpTrigger", auth_level=func.AuthLevel.FUNCTION)
def MyHttpTrigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
    
@app.route(route="GetProducts", auth_level=func.AuthLevel.FUNCTION)
def GetProducts(req: func.HttpRequest) -> func.HttpResponse:
    response = requests.get("https://api.mockfly.dev/mocks/85f777c7-5caf-41a2-9c31-3ce762be6265/api/products")
    print(response.json()[0])

    return func.HttpResponse(
        json.dumps(response.json(), indent=4),
        status_code=200
    )
    
@app.route(route="GetProducts", auth_level=func.AuthLevel.FUNCTION)
def GetProducts(req: func.HttpRequest) -> func.HttpResponse:
    response = requests.get("https://api.mockfly.dev/mocks/85f777c7-5caf-41a2-9c31-3ce762be6265/api/products")
    print(response.json()[0])

    return func.HttpResponse(
        json.dumps(response.json(), indent=4),
        status_code=200
    )