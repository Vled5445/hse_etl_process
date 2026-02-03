from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os

def iot_analysis():
    
    input_path = '/opt/airflow/data/IOT-temp.csv'
    
    df = pd.read_csv(input_path)
    print(f"Загружено: {len(df)} строк")
    
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M')
    df['date'] = df['noted_date'].dt.date  # тип date
    
    df = df[df['out/in'].str.strip().str.upper() == 'IN']
    print(f"После фильтрации 'In': {len(df)} строк")
    
    low, high = df['temp'].quantile([0.05, 0.95])
    df = df[(df['temp'] >= low) & (df['temp'] <= high)]
    print(f"После очистки по процентилям: {len(df)} строк")
    
    daily = df.groupby('date').agg(
        avg_temp=('temp', 'mean'),
        max_temp=('temp', 'max'),
        min_temp=('temp', 'min'),
        count=('temp', 'count')
    ).round(2).reset_index()
    
    hottest = daily.nlargest(5, 'max_temp')
    coldest = daily.nsmallest(5, 'min_temp')
    
    output_dir = '/opt/airflow/data'
    
    df.to_csv(f'{output_dir}/processed_iot_data.csv', index=False)
    hottest.to_csv(f'{output_dir}/hottest_days.csv', index=False)
    coldest.to_csv(f'{output_dir}/coldest_days.csv', index=False)
    
    print("\n" + "="*50)
    print("РЕЗУЛЬТАТЫ:")
    print("="*50)
    
    print("\n5 самых жарких дней:")
    for i, (_, row) in enumerate(hottest.iterrows(), 1):
        print(f"{i}. {row['date']}: макс {row['max_temp']}°C")
    
    print("\n5 самых холодных дней:")
    for i, (_, row) in enumerate(coldest.iterrows(), 1):
        print(f"{i}. {row['date']}: мин {row['min_temp']}°C")
    
    print(f"\nФайлы сохранены в {output_dir}/")

with DAG(
    'iot_simple_analysis',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['iot']
) as dag:
    
    task = PythonOperator(
        task_id='analyze',
        python_callable=simple_iot_analysis
    )
