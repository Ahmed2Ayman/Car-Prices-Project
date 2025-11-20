import pandas as pd
import requests
import os 

mdl=open("/opt/airflow/dags/model_dim.csv",'r')
x=mdl.read()
print(x)
mdl.close()
x=x.replace('\n$','|')

mdl=open("/opt/airflow/dags/model_dim.csv",'w')
mdl.write(x)
mdl.close()

mdl = pd.read_csv("/opt/airflow/dags/model_dim.csv")

mdl['price']=mdl['price'].str.split('|').str[0]

mdl['price']=mdl['price'].str.replace(',', '')
mdl.head(10)
mdl['price']=mdl['price'].astype("float")


url = "https://open.er-api.com/v6/latest/USD"
response = requests.get(url)
data = response.json()

rate = data["rates"]["EGP"]

mdl['price']=mdl['price']*rate

mdl['model_ID']=mdl['model_ID'].astype("object")
mdl['brand_ID']=mdl['brand_ID'].astype("object")


mdl['model_ID'] = mdl['model_ID'].apply(lambda x: '0' + str(x) if len(str(x)) == 3 else str(x))


mdl.rename(columns={'price': 'priceUSA'}, inplace=True)

br = pd.read_csv("/opt/airflow/dags/brand_dim.csv")
dbrands = sorted(br['brandName'].tolist(), key=len, reverse=True)
car_df = pd.read_csv("/opt/airflow/dags/car_prices.csv")
car_df
car_df[['brand', 'model']] = car_df['Classes'].apply(lambda x: pd.Series(str(x).split(maxsplit=1)) if ' ' in str(x) else car_df.Series([x, '']))
car_df

car_df['brand']=car_df['brand'].replace('Mercedes', 'Mercedes-Benz')

car_df['brand']=car_df['brand'].replace('Alfa', 'Alfa Romeo')
car_df['model']=car_df['model'].replace('Romeo ', '')

car_df['brand']=car_df['brand'].replace('Land', 'Land Rover')
car_df['model']=car_df['model'].replace('Rover ', '')

car_df['Old Price'] = car_df['Old Price'].str.replace('\n', '', regex=False).str.replace('EGP', '', regex=False)
car_df['New Price'] = car_df['New Price'].str.replace('\n', '', regex=False).str.replace('EGP', '', regex=False)
car_df['Price Change'] = car_df['Price Change'].str.replace('\n', '', regex=False).str.replace('EGP', '', regex=False).str.replace('trending_down', '', regex=False).str.replace('trending_up', '', regex=False).str.replace('+', '', regex=False)

car_df['Old Price'] = car_df['Old Price'].str.replace(',', '', regex=False)
car_df['New Price'] = car_df['New Price'].str.replace(',', '', regex=False)
car_df['Price Change'] = car_df['Price Change'].str.replace(',', '', regex=False)

car_df['Old Price'] = car_df['Old Price'].astype(float)
car_df['New Price'] = car_df['New Price'].astype(float)
car_df['Price Change'] = car_df['Price Change'].astype(float)

cleaned_folder = '/opt/airflow/dags/cleaned'
os.makedirs(cleaned_folder, exist_ok=True)

mdl.to_csv(os.path.join(cleaned_folder, 'model_price_USA.csv'), index=False) 

car_df.to_csv(os.path.join(cleaned_folder, 'model_price_Egypt.csv'), index=False)