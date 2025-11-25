FROM mcr.microsoft.com/azure-functions/python:4-python3.10

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

# SET WORKING DIRECTORY (PENTING)
WORKDIR /home/site/wwwroot

# Copy requirements & Install
COPY requirements.txt .
RUN pip install -r requirements.txt

# COPY SEMUA FILE (Termasuk folder utils dan function_app.py)
COPY . .