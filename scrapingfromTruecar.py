from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import time
import pandas as pd

chrome_options = Options()
chrome_options.add_argument("--headless") 
chrome_options.add_argument("--no-sandbox") 
chrome_options.add_argument("--disable-dev-shm-usage") 
chrome_options.add_argument("--disable-gpu")
drv=webdriver.Chrome(options=chrome_options)
url='https://www.truecar.com/shop/new/?filterType=brand'
drv.get(url)
carsB=drv.find_elements('xpath',"//div[@class='flex flex-row']/div")

brand=[]
for i in carsB:
    txt=i.text
    brand.append(txt)
del brand[0]

brandID=list(range(1,len(brand)+1))
brand_dim = pd.DataFrame(columns=["brandID","brandName"])
brand_dim['brandID']=brandID
brand_dim['brandName']=brand
brand_dim

url='https://www.truecar.com/shop/new/?filterType=brand&makeSlug='+brand[0].lower().replace(' ','-')
drv.get(url)


carsM=drv.find_elements('xpath',"//div[@class='h-full py-32']/div/a")

modelID=[]
brandMID=[]
model=[]
price=[]
n=1
for i in brand:
    url='https://www.truecar.com/shop/new/?filterType=brand&makeSlug='+i.lower().replace(' ','-')
    drv.get(url)
    time.sleep(2)
    carsM=drv.find_elements('xpath',"//div[@class='h-full py-32']/div/a")
    print(len(carsM))
    m=1
    for j in carsM:
        txt=j.text
        txt=txt.split('\nStarting at $')
        modelID.append(100*n+m)
        brandMID.append(n)
        model.append(txt[0])
        price.append(txt[1])
        m=m+1
    n=n+1



model_dim = pd.DataFrame(columns=["model_ID","brand_ID","brand_Name","price"])
model_dim['model_ID']=modelID
model_dim['brand_ID']=brandMID
model_dim['brand_Name']=model
model_dim['price']=price
model_dim

brand_dim.to_csv("/opt/airflow/dags/brand_dim.csv", index=False) 
model_dim.to_csv("/opt/airflow/dags/model_dim.csv", index=False)