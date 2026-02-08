from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
import os
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def full_load_to_target():
    source_dir = '/opt/airflow/data'
    target_dir = '/opt/airflow/data/target'
    
    os.makedirs(target_dir, exist_ok=True)
    
    processed_file = os.path.join(source_dir, 'processed_iot_data.csv')
    hottest_file = os.path.join(source_dir, 'hottest_days.csv')
    coldest_file = os.path.join(source_dir, 'coldest_days.csv')
    df = pd.read_csv(processed_file)
    hottest = pd.read_csv(hottest_file)
    coldest = pd.read_csv(coldest_file)
    
    print(f"Загружено {len(df)} строк измерений")
    print(f"Загружено {len(hottest)} самых жарких дней")
    print(f"Загружено {len(coldest)} самых холодных дней")
    
    
    target_main = os.path.join(target_dir, 'iot_measurements_full.csv')
    df.to_csv(target_main, index=False)
    
    target_hot = os.path.join(target_dir, 'iot_hot_days_full.csv')
    hottest.to_csv(target_hot, index=False)
    
    target_cold = os.path.join(target_dir, 'iot_cold_days_full.csv')
    coldest.to_csv(target_cold, index=False)
    
    hottest['type'] = 'hot'
    coldest['type'] = 'cold'
    extreme_days = pd.concat([hottest, coldest], ignore_index=True)
    target_extreme = os.path.join(target_dir, 'iot_extreme_days_full.csv')
    extreme_days.to_csv(target_extreme, index=False)
    
    metadata = {
        'load_type': 'full',
        'load_timestamp': datetime.now().isoformat(),
        'source_files': {
            'processed_data': 'processed_iot_data.csv',
            'hottest_days': 'hottest_days.csv',
            'coldest_days': 'coldest_days.csv'
        },
        'target_files': {
            'measurements': 'iot_measurements_full.csv',
            'hot_days': 'iot_hot_days_full.csv',
            'cold_days': 'iot_cold_days_full.csv',
            'extreme_days': 'iot_extreme_days_full.csv'
        },
        'statistics': {
            'total_records': int(len(df)),
            'hottest_days_count': int(len(hottest)),
            'coldest_days_count': int(len(coldest)),
            'date_min': str(df['date'].min()) if 'date' in df.columns else 'N/A',
            'date_max': str(df['date'].max()) if 'date' in df.columns else 'N/A'
        }
    }
    
    metadata_file = os.path.join(target_dir, 'full_load_metadata.json')
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    return {
        'status': 'success',
        'records_loaded': len(df),
        'target_files': ['iot_measurements_full.csv', 'iot_extreme_days_full.csv']
    }

with DAG(
    'iot_full_load',
    default_args=default_args,
    description='Полная загрузка исторических данных IoT в целевую систему',
    schedule=None, 
    catchup=False,
    tags=['iot', 'full_load', 'etl', 'homework']
) as dag:
    
    start_task = EmptyOperator(task_id='start_full_load')
    
    load_task = PythonOperator(
        task_id='full_load_data',
        python_callable=full_load_to_target,
    )
    
    end_task = EmptyOperator(task_id='end_full_load')
    
    start_task >> load_task >> end_task
