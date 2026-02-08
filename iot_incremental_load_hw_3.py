from datetime import datetime, timedelta
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

def get_last_load_date():
    
    metadata_file = '/opt/airflow/data/target/incremental_metadata.json'
    
    if os.path.exists(metadata_file):
        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            last_date = metadata.get('last_load_date')
            if last_date:
                return {
                    'last_load_date': last_date,
                    'is_first_load': False
                }
        except:
            pass
    
    return {
        'last_load_date': None,
        'is_first_load': True
    }

def incremental_load_to_target(**context):
    ti = context['ti']
    date_info = get_last_load_date()
    
    if date_info['is_first_load']:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=3)
    else:
        last_date = datetime.fromisoformat(date_info['last_load_date']).date()
        start_date = last_date
        end_date = datetime.now().date()

    source_file = '/opt/airflow/data/IOT-temp.csv'
    
    df = pd.read_csv(source_file)
    
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M')
    df['date'] = df['noted_date'].dt.date
    
    df['date'] = pd.to_datetime(df['date'])
    mask = (df['date'] >= pd.Timestamp(start_date)) & (df['date'] <= pd.Timestamp(end_date))
    df = df[mask]
    
    if len(df) == 0:
        print("Нет новых данных для загрузки")
        return {'status': 'no_new_data'}
    
    df = df[df['out/in'].str.strip().str.upper() == 'IN']
    
    low, high = df['temp'].quantile([0.05, 0.95])
    df = df[(df['temp'] >= low) & (df['temp'] <= high)]
    
    daily = df.groupby('date').agg(
        avg_temp=('temp', 'mean'),
        max_temp=('temp', 'max'),
        min_temp=('temp', 'min'),
        count=('temp', 'count')
    ).round(2).reset_index()
    
    hottest = daily.nlargest(5, 'max_temp')
    coldest = daily.nsmallest(5, 'min_temp')
    
    target_dir = '/opt/airflow/data/target/incremental'
    os.makedirs(target_dir, exist_ok=True)
    
    existing_file = '/opt/airflow/data/target/iot_measurements_full.csv'
    if os.path.exists(existing_file):
        existing_df = pd.read_csv(existing_file)
        combined_df = pd.concat([existing_df, df], ignore_index=True).drop_duplicates()
    else:
        combined_df = df
    
    target_file = os.path.join(target_dir, f'iot_measurements_incremental_{end_date}.csv')
    combined_df.to_csv(target_file, index=False)
    
    if len(hottest) > 0:
        hot_file = os.path.join(target_dir, f'hot_days_{end_date}.csv')
        hottest.to_csv(hot_file, index=False)
    
    if len(coldest) > 0:
        cold_file = os.path.join(target_dir, f'cold_days_{end_date}.csv')
        coldest.to_csv(cold_file, index=False)
    
    metadata = {
        'last_load_date': end_date.isoformat(),
        'load_timestamp': datetime.now().isoformat(),
        'load_type': 'incremental',
        'period': {
            'start': start_date.isoformat(),
            'end': end_date.isoformat()
        },
        'statistics': {
            'new_records': int(len(df)),
            'total_records': int(len(combined_df)),
            'hottest_days': int(len(hottest)),
            'coldest_days': int(len(coldest))
        }
    }
    
    metadata_file = '/opt/airflow/data/target/incremental_metadata.json'
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
  
    
    return {
        'status': 'success',
        'new_records': len(df),
        'period': f"{start_date} - {end_date}"
    }

with DAG(
    'iot_incremental_load',
    default_args=default_args,
    description='Инкрементальная загрузка изменений IoT данных',
    schedule='0 2 * * *', 
    catchup=False,
    tags=['iot', 'incremental_load', 'etl', 'homework']
) as dag:
    
    start_task = EmptyOperator(task_id='start_incremental_load')
    
    get_date_task = PythonOperator(
        task_id='get_last_load_date',
        python_callable=get_last_load_date,
    )
    
    load_task = PythonOperator(
        task_id='incremental_load_data',
        python_callable=incremental_load_to_target,
    )
    
    end_task = EmptyOperator(task_id='end_incremental_load')
    
    start_task >> get_date_task >> load_task >> end_task
