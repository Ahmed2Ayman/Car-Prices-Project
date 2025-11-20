import requests
from bs4 import BeautifulSoup
import pandas as pd 

def get_table_header(table_element):
    header_row = table_element.find("thead").find("tr") 
    if header_row:
        headers = [th.text.strip() for th in header_row.find_all("th")]
        return headers
    return []
def get_prices_data(table_element):
    
    all_rows_data = []
    prices_data_body = table_element.find("tbody")
    if not prices_data_body: 
        return []
        
    all_rows = prices_data_body.find_all("tr")
    for row in all_rows:
        prices_data_tags = row.find_all("td")
        prices_data_text = [cell.text.strip() for cell in prices_data_tags]
        if prices_data_text:
            all_rows_data.append(prices_data_text)
            
    return all_rows_data

def main_scrape_page(page_content):
    soup = BeautifulSoup(page_content, "lxml")
    main_table = soup.find("table", {'class': 'pricesChangeTable'}) 
    
    if main_table:
        page_data = get_prices_data(main_table)
        return page_data
    else:
        return [] 

all_data = [] 
headers = []

first_page_url = "https://eg.hatla2ee.com/en/new-car/price-change/page/1"
first_page_req = requests.get(first_page_url)

first_soup = BeautifulSoup(first_page_req.content, "lxml")
first_table = first_soup.find("table", {'class': 'pricesChangeTable'})
if first_table:
    headers = get_table_header(first_table) 

for i in range(1, 19): 
    page_url = f"https://eg.hatla2ee.com/en/new-car/price-change/page/{i}"
    page_req = requests.get(page_url)        
    page_specific_data = main_scrape_page(page_req.content)
    if page_specific_data: 
        all_data.extend(page_specific_data)



df = pd.DataFrame(all_data, columns=headers)
csv_file_path = '/opt/airflow/dags/car_prices.csv' 
df.to_csv(csv_file_path, index=False)