from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator 

with DAG(
    dag_id="car_prices_pipeline",
    schedule='@daily',
    start_date=pendulum.datetime(2025, 10, 24, tz="Africa/Cairo"), 
    catchup=False,
    tags=['car prices']
) as dag:
    scrap_from_truecar=BashOperator(
        task_id='scrap_truecar',
        bash_command='python /opt/airflow/dags/scrapingfromTruecar.py' 
    )
    scrap_from_hatla2ee=BashOperator(
        task_id="scrap_hatla2ee",
        
        bash_command="python /opt/airflow/dags/scrap_car_prices.py" 
    )

    transform_data=BashOperator(
        task_id="transform_data",

        bash_command="python /opt/airflow/dags/ETL.py" 
    )
    load_to_snowflake = BashOperator(
        task_id="load_to_snowflake",
        bash_command="python /opt/airflow/dags/load_to_snowflake.py" 
    )

    [scrap_from_truecar, scrap_from_hatla2ee] >> transform_data >>load_to_snowflake