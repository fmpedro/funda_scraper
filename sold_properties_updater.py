import requests
from requests.exceptions import Timeout
import time
import sqlite3
from bs4 import BeautifulSoup as bs
import datetime
import logging
import sys


# Logging configuration:
logging.basicConfig(
    level=logging.INFO,
    filename='funda_scraper.log',
    format='%(asctime)s | sold_properties_updater |  %(levelname)s - %(message)s')

errors_filename = 'errors.txt'

# Check if specific city was defined:
if len(sys.argv) < 2:
    additional_query = ""
else:
    additional_query = " " + sys.argv[1]


def get_listing_soup(listing_url):
    user_agent = '''Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'''
    headers = {
        "User-Agent": user_agent
    }
    response = requests.get(listing_url, headers = headers)
    if '/zoeken/' in response.url:
        return None
    else:
        return bs(response.text, "html.parser")


def get_listing_status(soup):
    status = soup.find('dt', string='Status')
    if status:
        status = status.find_next('dd').text.strip()
    else:
        status = None
    labels = soup.find("div", class_ = "object-header__labels")
    if labels:
        li_elements = soup.find("div", class_ = "object-header__labels").find_all('li')
        tags = ", ".join([li.get_text(strip=True) for li in li_elements])
    else:
        tags = ''
    blikvanger = soup.find("span", {"class": "label-blikvanger"})
    if blikvanger and 'Verkocht' not in status:
        if tags == '':
            tags = blikvanger.text.strip()
        else:
            tags = blikvanger.text.strip() + ', ' + tags
    return (status, tags)


# Connect to the database and get urls for properties marked as not sold
try:
    conn = sqlite3.connect("funda_properties.db")
    cursor = conn.cursor()

    # Get list of URLs of not sold properties:
    cursor.execute("SELECT id, url FROM scraped_properties WHERE sold = 0" + additional_query)
    url_list = cursor.fetchall()
    print(f"Sold properties update started at {datetime.datetime.now()}. Number of records to process: {len(url_list)}")
    logging.info(f'Funda Sold Properties update started. Number of records to process: {len(url_list)}')


except Exception as e:
    print(f"Error during database querying.")
    logging.error(f'Error during database querying: {e}')
    sys.exit(1)


# Scrape each property's url and check for status. If sold, update database
processed_records = 0
error_records = []

for property_id, property_url in url_list:
    try:
        today_string = datetime.date.today().strftime('%Y-%m-%d')
        soup = get_listing_soup(property_url)
        if soup == None:
            sold = True
            tags = 'Property Removed'
        else:
            status, tags = get_listing_status(soup)
            if status == None or 'Verkocht' in status or 'Verhuurd' in status:
                sold = True
            else:
                sold = False
        
        cursor.execute('''
            UPDATE scraped_properties SET sold=?, tags=?,  mutation_date=? WHERE id=?
            ''', (sold,
                  tags,
                  today_string,
                  property_id))
        conn.commit()
        processed_records += 1

    except Exception as e:
        error_records.append(f"{property_id} | {property_url} | {e}")

    time.sleep(1)

# Close the connection
try:
    conn.close()
    with open(errors_filename, 'w') as f:
        for record in error_records:
            f.write(f"{record}\n")

    print(f"Database updated at {datetime.datetime.now()}. {processed_records} records processed. {len(error_records)} errors.")
    logging.info(f'Database updated. {processed_records} records processed. {len(error_records)} errors.')

except Exception as e:
    print(f"Error during database update! {processed_records} records processed. {len(error_records)} errors.")
    logging.error(f'Error during database update: {e}. \n{processed_records} records processed. {len(error_records)} errors.')
