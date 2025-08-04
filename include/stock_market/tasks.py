from io import BytesIO
import json
import requests
from airflow.hooks.base import BaseHook
from minio import Minio
from airflow.exceptions import AirflowNotFoundException


BUCKET_NAME = "stock-market"

def _get_minio_client():
    minio = BaseHook.get_connection("minio")
    client = Minio(
        endpoint=minio.extra_dejson["endpoint_url"].split("//")[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client


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
    client = _get_minio_client()

    # 2. Make the bucket if it doesn't exist.
    found = client.bucket_exists(BUCKET_NAME)
    if not found:
        client.make_bucket(BUCKET_NAME)
        print("Created bucket", BUCKET_NAME)
    else:
        print("Bucket", BUCKET_NAME, "already exists")

    # 3. Transforma data to Dict
    stock = json.loads(stock)
    symbol = stock["meta"]["symbol"]
    data = json.dumps(stock, ensure_ascii=False).encode("utf-8")

    # 4. Upload the file, renaming it in the process
    objw = client.put_object(
        bucket_name=BUCKET_NAME, 
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data)
    )
    print(f"Object {objw.object_name} uploaded successfully to MinIO bucket {objw.bucket_name}")
    return f"{objw.bucket_name}/{symbol}"


def _get_formatted_csv(path):
    # 1. Connect to MinIO
    client = _get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    # 2. List objects in the bucket with the specified prefix
    objects = client.list_objects(
        bucket_name=BUCKET_NAME,
        prefix=prefix_name,
        recursive=True
    )
    # 3. Find the object that ends with .csv
    for obj in objects:
        if obj.object_name.endswith(".csv"):
            print(obj.object_name)
            return obj.object_name
    return AirflowNotFoundException("The csv file does not exists.")