from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from datetime import datetime

@dag(
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["stock_market"]
)


def stock_market():
    
    @task.sensor(poke_interval=30, timeout=300, mode="poke")
    def is_api_available() -> PokeReturnValue:
        """
        Comprueba si la API está disponible realizando una solicitud(request).
        """
        import requests

        api = BaseHook.get_connection("stock_api")
        url = f"{api.host}{api.extra_dejson["endpoint"]}"
        print(url)
        response = requests.get(url, headers=api.extra_dejson["headers"])
        condition = response.json()["finance"]["result"] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    is_api_available()

stock_market()