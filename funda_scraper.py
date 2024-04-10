import requests
from requests.exceptions import Timeout
import time
import sqlite3
from bs4 import BeautifulSoup as bs
import pickle
import re
import datetime
import logging
import sys
import pandas as pd


# Logging configuration:
logging.basicConfig(
    level=logging.INFO,
    filename='funda_scraper.log',
    format='%(asctime)s | %(levelname)s - %(message)s')


# Define a class to hold the extracted data
class PropertyListing:
    def __init__(self, name, type, postal_code_number, postal_code_letters, city, province, price, area, num_of_rooms, energy_rating, features, estate_agent, url, tags):
        self.name = name
        self.type = type
        self.postal_code_number = postal_code_number
        self.postal_code_letters = postal_code_letters
        self.city = city
        self.province = province
        self.price = price
        self.features_array = features
        self.area = area
        self.num_of_rooms = num_of_rooms
        self.energy_rating = energy_rating
        self.estate_agent = estate_agent
        self.url = url
        self.tags = tags

    # method to ensure that there are no new objects added to results array that are already in there
    def __eq__(self, other):
        return (
            isinstance(other, PropertyListing) and 
            self.name == other.name and
            self.city == other.city
        )

    def __hash__(self):
        return hash((self.name, self.city))


def scrape_page(url, timeout=30):
    user_agent = '''Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'''
    headers = {
        "User-Agent": user_agent
    }
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
    except Timeout:
        return None
    soup = bs(response.text, "html.parser")
    return soup.find_all("div", {"data-test-id": "search-result-item"})

        
def get_province(zipcode, ref_data):
    province = ref_data[ref_data.PC4 == int(zipcode)]['Provincie name'].values[0]
    return province


results_array = set()

ref_data = pd.read_csv('georef-netherlands-postcode-pc4.csv', sep='\t')


# development_mode = False
# if development_mode: # url list used for testing
#     base_url_list = ['https://www.funda.nl/zoeken/koop?selected_area=%5B"hollum"%5D&object_type=%5B"house"%5D&sort="date_down"&search_result=']
# else: # url list used in production
#     base_url_list = [
#         'https://www.funda.nl/zoeken/koop?selected_area=%5B"gemeente-amsterdam,10km"%5D&object_type=%5B"apartment","house"%5D&sort="date_down"&search_result=',
#         'https://www.funda.nl/zoeken/koop?selected_area=%5B"gemeente-utrecht,10km"%5D&object_type=%5B"apartment","house"%5D&sort="date_down"&search_result=',
#         'https://www.funda.nl/zoeken/koop?selected_area=%5B"gemeente-alkmaar,10km"%5D&object_type=%5B"apartment","house"%5D&sort="date_down"&search_result=',
#         'https://www.funda.nl/zoeken/koop?selected_area=%5B"gemeente-haarlem,5km"%5D&object_type=%5B"apartment","house"%5D&sort="date_down"&search_result='
#         ]
base_url = 'https://www.funda.nl/zoeken/koop?selected_area=%5B"nl"%5D&object_type=%5B"apartment","house"%5D&sort="date_down"&availability=%5B"available","negotiations","unavailable"%5D&publication_date="5"&search_result='
    

try:
    print(f"Scraping started at {datetime.datetime.now()}. URL: {base_url}")
    logging.info(f'Funda scraping started (URL: {base_url})')

    page_number = 0
    max_pages = 666
    carry_on = True
    
    while carry_on and page_number <= max_pages-1:
        page_number += 1
        url = base_url + str(page_number)
        
        search_result_items = scrape_page(url)

        if search_result_items == None:
            continue
        
        if len(search_result_items) == 0:
            carry_on = False
        else:
            print(page_number, end='\r')
            for item in search_result_items:
                # property name:
                property_name = item.find("h2", {"data-test-id": "street-name-house-number"}).get_text().strip()

                # property type:
                property_url = item.find("a").get("href")
                if re.findall(r'/appartement-', property_url):
                    property_type = "apartment"
                elif re.findall(r'/huis-', property_url):
                    property_type = "house"
                else:
                    property_type = "unknown"

                # postal code:
                postal_code_city = item.find("div", {"data-test-id": "postal-code-city"}).get_text().strip().split(' ',3)
                postal_code_number = postal_code_city[0]
                postal_code_letters = postal_code_city[1]
                
                # city
                city = " ".join(postal_code_city[2:])
                
                # price:
                price_sale_array = item.find("p", {"data-test-id": "price-sale"}).get_text().strip().split(' ',3)
                try:
                    price = float(price_sale_array[1].replace('.', ''))
                except:
                    price = None
                
                # url:
                url_link = item.find("a")["href"].strip()
                
                # additional info:
                additional_info = item.find("ul", {"class": "mt-1 flex h-6 min-w-0 flex-wrap overflow-hidden"})
                aditional_sub_items = additional_info.find_all("li")
                features = [sub_item.get_text().strip() for sub_item in aditional_sub_items]
                
                # area:
                if "m²" in features[0]:
                    area = float(features[0].replace(" m²", ""))
                else:
                    area = None
                
                # number of rooms:
                try:
                    if "m²" not in features[1]:
                        num_of_rooms = int(features[1])
                    else:
                        if re.match("[A-Z]+", features[2]):
                            num_of_rooms = None
                        else:
                            num_of_rooms = int(features[2])
                except:
                    num_of_rooms = None
                
                # energy rating:
                try:
                    if re.match("[A-Z]+", features[2]):
                        energy_rating = features[2]
                    else:
                        energy_rating = features[3]
                except:
                    energy_rating = None

                # estate agent
                property_estate_agent = item.find("div", {"class": "mt-4 flex"}).get_text().strip()

                # tags:
                ul_element = item.find('ul', class_='absolute left-2 top-2 flex w-56 flex-wrap')
                li_elements = ul_element.find_all('li')
                tags = ",".join([li.get_text(strip=True) for li in li_elements])

                # append property listing to results array:
                results_array.add(PropertyListing(
                    name=property_name,
                    type=property_type,
                    postal_code_number=postal_code_number,
                    postal_code_letters=postal_code_letters,
                    city=city,
                    province=get_province(postal_code_number, ref_data),
                    price=price,
                    area=area,
                    num_of_rooms=num_of_rooms,
                    energy_rating=energy_rating,
                    url=url_link,
                    features=features,
                    estate_agent=property_estate_agent,
                    tags=tags              
                ))
            time.sleep(10)

    logging.info(f'Operation completed. {page_number} result page(s) processed.)')

except Exception as e:
    print(f"Error during operation!")
    logging.error(f'Error during operation on page {page_number}: {e}')

    
print(f"Operation ended at {datetime.datetime.now()}")


with open('scraped_data.pkl', 'wb') as file:
	pickle.dump(results_array, file)


today = datetime.date.today()
today_string = today.strftime('%Y-%m-%d')


# Create or connect to the database
try:
    conn = sqlite3.connect("funda_properties.db")
    cursor = conn.cursor()

    # Define database table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scraped_properties (
            id INTEGER PRIMARY KEY,
            name TEXT,
            type TEXT,
            postal_code_number TEXT,
            postal_code_letters TEXT,
            city TEXT,
            province TEXT,
            price REAL,
            area REAL,
            num_of_rooms INTEGER,
            energy_rating TEXT,
            url TEXT,
            estate_agent TEXT,
            sold BOOL,
            tags TEXT,
            creation_date DATE DEFAULT CURRENT_DATE,
            mutation_date DATE DEFAULT CURRENT_DATE
        )
    ''')

    # Get list of IDs of properties in the database:
    cursor.execute("SELECT name || ',' || city AS name_city_key, sold FROM scraped_properties")
    name_city_list = {row[0]: row[1] for row in cursor.fetchall()}

    # Insert objects into the table
    for property_obj in results_array:
        key = property_obj.name + "," + property_obj.city
        if property_obj.tags == "Verkocht":
            sold = True
        else:
            sold = False
 
        # if property is already in the database:
        if key in name_city_list.keys() and name_city_list[key] == 0:
            cursor.execute('''
                UPDATE scraped_properties SET price=?, area=?, num_of_rooms=?, energy_rating=?, url=?, tags=?, sold=?, mutation_date=? WHERE name=? AND city=?
                ''', (property_obj.price,
                      property_obj.area,
                      property_obj.num_of_rooms,
                      property_obj.energy_rating,
                      property_obj.url,
                      property_obj.tags,
                      sold,
                      today_string,
                      property_obj.name,
                      property_obj.city))

        # if it is a new property:
        else:
            cursor.execute('''
                    INSERT INTO scraped_properties (name, type, postal_code_number, postal_code_letters, city, province, price, area, num_of_rooms, energy_rating, url, estate_agent, tags, sold, creation_date, mutation_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (property_obj.name,
                      property_obj.type,
                      property_obj.postal_code_number,
                      property_obj.postal_code_letters,
                      property_obj.city,
                      property_obj.province,
                      property_obj.price,
                      property_obj.area,
                      property_obj.num_of_rooms,
                      property_obj.energy_rating,
                      property_obj.url,
                      property_obj.estate_agent,
                      property_obj.tags,
                      sold,
                      today_string,
                      today_string))

    # # Remove duplicate entries from the database
    # cursor.execute('''
    #     DELETE FROM scraped_properties
    #     WHERE id > (
    #             SELECT MIN(id) FROM scraped_properties sp2
    #             WHERE scraped_properties.name = sp2.name
    #             AND scraped_properties.city = sp2.city
    #     )
    #     ''')


    # Commit changes and close the connection
    conn.commit()
    conn.close()

    print(f"Database updated at {datetime.datetime.now()}")
    logging.info(f'Database updated.')

except Exception as e:
    try:
        conn.close()
    except:
        pass
    print(f"Error during database update: {e}")
    logging.error(f'Error during database update: {e}')
