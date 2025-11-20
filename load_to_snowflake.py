import snowflake.connector
import os 

SNOWFLAKE_ACCOUNT = 'SNOWFLAKE_ACCOUNT'
SNOWFLAKE_USER = 'SNOWFLAKE_USER'
SNOWFLAKE_PASSWORD = 'SNOWFLAKE_PASSWORD' 
SNOWFLAKE_WAREHOUSE = 'SNOWFLAKE_WAREHOUSE'
SNOWFLAKE_DATABASE = 'SNOWFLAKE_DATABASE'
SNOWFLAKE_SCHEMA = 'SNOWFLAKE_SCHEMA'

usa_prices_file_local_path ="/opt/airflow/dags/cleaned/model_price_USA.csv"
egypt_prices_file_local_path = "/opt/airflow/dags/cleaned/model_price_Egypt.csv"

USA_TABLE_NAME = 'MODEL_PRICE_USA' 
EGYPT_TABLE_NAME = 'MODEL_PRICE_EGYPT' 

usa_staged_file_name = os.path.basename(usa_prices_file_local_path)
egypt_staged_file_name = os.path.basename(egypt_prices_file_local_path)

USA_TABLE_NAME_UPPER = USA_TABLE_NAME.upper()
EGYPT_TABLE_NAME_UPPER = EGYPT_TABLE_NAME.upper()
usa_internal_stage_name = f'@%{USA_TABLE_NAME_UPPER}' 
egypt_internal_stage_name = f'@%{EGYPT_TABLE_NAME_UPPER}' 

conn = None
try:
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cur = conn.cursor()

    put_sql_usa = f"PUT file://{usa_prices_file_local_path.replace(os.sep, '/')} {usa_internal_stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    cur.execute(put_sql_usa)
    copy_sql_usa = f"""
        COPY INTO {USA_TABLE_NAME_UPPER}
        FROM '{usa_internal_stage_name}/{usa_staged_file_name}.gz' 
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        PURGE = TRUE; 
    """ 
    cur.execute(copy_sql_usa)
    put_sql_egypt = f"PUT file://{egypt_prices_file_local_path.replace(os.sep, '/')} {egypt_internal_stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"

    cur.execute(put_sql_egypt)

    copy_sql_egypt = f"""
        COPY INTO {EGYPT_TABLE_NAME_UPPER} 
        FROM '{egypt_internal_stage_name}/{egypt_staged_file_name}.gz' 
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        PURGE = TRUE; 
    """ 

    cur.execute(copy_sql_egypt)



except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if 'cur' in locals() and cur:
        cur.close()
    if conn:
        conn.close()

        print("Connection closed.")
