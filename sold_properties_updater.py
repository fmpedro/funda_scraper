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


def get_listing_status(listing_url):
    user_agent = '''Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'''
    headers = {
        "User-Agent": user_agent
    }
    response = requests.get(listing_url, headers = headers)
    soup = bs(response.text, "html.parser")
    status = soup.find('dt', string='Status')
    if status:
        return status.find_next('dd').text.strip()
    else:
        return None


def get_province(zipcode, ref_data):
    province = ref_data[ref_data.PC4 == int(zipcode)]['Provincie name'][0]
    return province


# Connect to the database and get urls for properties marked as not sold
try:
    print(f"Sold properties update started at {datetime.datetime.now()}.")
    logging.info(f'Funda Sold Properties update started.')

    conn = sqlite3.connect("funda_properties.db")
    cursor = conn.cursor()

    # Get list of URLs of not sold properties:
    cursor.execute("SELECT id, url FROM scraped_properties WHERE sold = 0")
    url_list = cursor.fetchall()

except Exception as e:
    print(f"Error during database querying.")
    logging.error(f'Error during database querying: {e}')

url_list = url_list[0:5]


# Scrape each property's url and check for status. If sold, update database
for property_id, property_url in url_list:
    try:
        status = get_listing_status(property_url)
        print(property_url, status)
        if 'Verkocht' in status:
            cursor.execute('''
                UPDATE scraped_properties SET sold=?, tags=? WHERE id=?
                ''', (True,
                      status,
                      property_id))

    except Exception as e:
        print(f"Error during scraping/update of property with id: {property_id}.")
        logging.error(f'Error during scraping/update of property with id: {property_id}: {e}')

    time.sleep(5)

# Commit changes and close the connection
try:
    conn.commit()
    conn.close()

    print(f"Database updated at {datetime.datetime.now()}")
    logging.info(f'Database updated.')

except Exception as e:
    print(f"Error during database update!")
    logging.error(f'Error during database update: {e}')