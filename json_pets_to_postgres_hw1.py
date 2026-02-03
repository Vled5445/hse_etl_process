import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


def load_and_flatten_json():
    url = "https://raw.githubusercontent.com/LearnWebCode/json-example/master/pets-data.json"
    data = requests.get(url).json()

    flat_rows = []

    for pet in data["pets"]:
        flat_rows.append({
            "name": pet.get("name"),
            "species": pet.get("species"),
            "fav_foods": ", ".join(pet.get("favFoods", [])),
            "birth_year": pet.get("birthYear"),
            "photo_url": pet.get("photo")
        })

    df = pd.DataFrame(flat_rows)

    postgres_hook = PostgresHook(postgres_conn_id="my_pg")
    engine = postgres_hook.get_sqlalchemy_engine()

    df.to_sql(
        name="pets_flat",
        con=engine,
        if_exists="replace",
        index=False
    )


with DAG(
    dag_id="json_pets_to_postgres",
    schedule="@once",
    start_date=datetime.strptime('2026-01-25', '%Y-%m-%d'),
    catchup=False,
) as dag:

    flatten_and_load = PythonOperator(
        task_id="flatten_json_and_load_to_postgres",
        python_callable=load_and_flatten_json
    )
