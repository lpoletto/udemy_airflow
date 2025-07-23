from io import BytesIO
import json
import requests
from airflow.hooks.base import BaseHook
from minio import Minio

def _get_stock_prices(url, symbol):
    """
    Get stock prices for a given symbol from the specified URL.
    e.g.: 
    symbol=MELI 
    url=https://query1.finance.yahoo.com/v8/finance/chart/meli?metrics=high?&interval=1d&range=1y
    """
    
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection("stock_api")
    response = requests.get(url, headers=api.extra_dejson["headers"])
    return json.dumps(response.json()["chart"]["result"][0])


def _store_prices(stock):
    # 1. Connect to MinIO
    minio = BaseHook.get_connection("minio")
    client = Minio(
        endpoint=minio.extra_dejson["endpoint_url"].split("//")[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )

    # 2. The destination bucket on the MinIO server
    bucket_name = "stock-market"
    # 3. Make the bucket if it doesn't exist.
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")

    # 4. Transforma data to Dict
    stock = json.loads(stock)
    symbol = stock["meta"]["symbol"]
    data = json.dumps(stock, ensure_ascii=False).encode("utf-8")

    # 5. Upload the file, renaming it in the process
    objw = client.put_object(
        bucket_name=bucket_name, 
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data)
    )
    print(f"Object {objw.object_name} uploaded successfully to MinIO bucket {objw.bucket_name}")
    return f"{objw.bucket_name}/{symbol}"
    