import requests
from requests.exceptions import Timeout
import time
from bs4 import BeautifulSoup as bs
import pickle
import re
import datetime
import logging
import sys
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, Date, Text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logging configuration:
logging.basicConfig(
    level=logging.INFO,
    filename='funda_scraper.log',
    format='%(asctime)s | funda_scraper | %(levelname)s - %(message)s')

# SQLAlchemy setup
Base = declarative_base()

class ScrapedProperty(Base):
    __tablename__ = 'scraped_properties'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    type = Column(String(50))
    postal_code_number = Column(String(10))
    postal_code_letters = Column(String(5))
    city = Column(String(100))
    province = Column(String(100))
    gemeente = Column(String(100))
    price = Column(Float)
    area = Column(Float)
    num_of_rooms = Column(Integer)
    energy_rating = Column(String(10))
    url = Column(Text)
    estate_agent = Column(String(255))
    sold = Column(Boolean, default=False)
    tags = Column(Text)
    creation_date = Column(Date, default=datetime.date.today)
    mutation_date = Column(Date, default=datetime.date.today)

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL')

# Create engine and session
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create tables
Base.metadata.create_all(bind=engine)


# Define a class to hold the extracted data
class PropertyListing:
    def __init__(self, name, type, postal_code_number, postal_code_letters, city, province, gemeente, price, area, num_of_rooms, energy_rating, features, estate_agent, url, tags):
        self.name = name
        self.type = type
        self.postal_code_number = postal_code_number
        self.postal_code_letters = postal_code_letters
        self.city = city
        self.province = province
        self.gemeente = gemeente
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
    user_agent = '''Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)'''
    headers = {
        "User-Agent": user_agent
    }
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
    except Timeout:
        return None
    soup = bs(response.text, "html.parser")
    return soup.select("div.gap-3:nth-child(3) > div")

        
def get_zipcode_info(zipcode, ref_data):
    try:
        zipcode_int = int(zipcode)
        province_series = ref_data[ref_data.PC4 == zipcode_int]['Provincie name']
        gemeente_series = ref_data[ref_data.PC4 == zipcode_int]['Gemeente name']
        
        province = province_series.values[0] if len(province_series) > 0 else ""
        gemeente = gemeente_series.values[0] if len(gemeente_series) > 0 else ""
        
        return {"province": province, "gemeente": gemeente}
    except (ValueError, IndexError):
        return {"province": "", "gemeente": ""}


# =============== FUNDA SCRAPPING ===============
results_array = set()

ref_data = pd.read_csv('georef-netherlands-postcode-pc4.csv', sep='\t')

base_url_list = [
    'https://www.funda.nl/zoeken/koop?selected_area=[%22nl%22]&object_type=[%22house%22,%22apartment%22]&publication_date=%225%22&search_result=',
    ] 

for base_url in base_url_list:
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
                    try:
                        link_tag = item.select_one('h2 a[data-testid="listingDetailsAddress"]')

                        # URL:
                        url_link = "https://www.funda.nl" + link_tag["href"]
                        
                        # property name:
                        property_name = item.select_one("h2 a div.flex.font-semibold span.truncate").get_text(strip=True)

                        # property type:
                        if re.findall(r'/appartement-', url_link):
                            property_type = "apartment"
                        elif re.findall(r'/huis-', url_link):
                            property_type = "house"
                        else:
                            property_type = "unknown"

                        # postal code + city:
                        postal_city = link_tag.select_one("div.truncate.text-neutral-80").get_text(strip=True)
                        parts = postal_city.split()
                        postal_code_number, postal_code_letters = parts[0], parts[1]
                        city = " ".join(parts[2:])
                        
                        # Price
                        price_text = item.select_one("div.mt-2 .truncate").get_text(strip=True)
                        price = None
                        if price_text.startswith("€"):
                            try:
                                price = float(price_text.split()[1].replace(".", ""))
                            except:
                                pass

                        # Features (area, plot size, rooms, energy label)
                        features = [li.get_text(strip=True) for li in item.select("ul li")]
                        area = None
                        num_of_rooms = None
                        energy_rating = None
                        try:
                            for f in features:
                                if "m²" in f and area is None:
                                    area = float(f.replace(" m²", "").replace(",", "."))
                                elif f.isdigit():
                                    num_of_rooms = int(f)
                                elif re.match(r"^[A-Z]+\+*$", f):
                                    energy_rating = f
                        except:
                            pass
                        
                        # Estate agent
                        try:
                            estate_agent = item.select_one('a[href*="/makelaar/"] span').get_text(strip=True)
                        except:
                            estate_agent = None
                            
                        # Tags
                        tags = ", ".join([tag.get_text(strip=True) for tag in item.select('div.absolute[class*="left-2"] span')])

                        # append property listing to results array:
                        results_array.add(PropertyListing(
                            name=property_name,
                            type=property_type,
                            postal_code_number=postal_code_number,
                            postal_code_letters=postal_code_letters,
                            city=city,
                            province=get_zipcode_info(postal_code_number, ref_data)['province'],
                            gemeente=get_zipcode_info(postal_code_number, ref_data)['gemeente'],
                            price=price,
                            area=area,
                            num_of_rooms=num_of_rooms,
                            energy_rating=energy_rating,
                            url=url_link,
                            features=features,
                            estate_agent=estate_agent,
                            tags=tags              
                        ))
                    except Exception as e:
                        logging.warning(f'Error processing item on page {page_number}: {e}')
                        continue
                        
                time.sleep(10)

        logging.info(f'Scraping completed. {page_number} result page(s) processed.)')

    except Exception as e:
        print(f"Error during operation!")
        logging.error(f'Error during operation on page {page_number}: {e}')


# Backup scraped data
with open('scraped_data.pkl', 'wb') as file:
    pickle.dump(results_array, file)


# =============== DATABASE OPERATIONS ===============
try:
    session = SessionLocal()
    
    # Prepare data for bulk insert
    properties_data = []
    for property_obj in results_array:
        # Clean the data
        province = property_obj.province if property_obj.province is not None else ''
        gemeente = property_obj.gemeente if property_obj.gemeente is not None else ''
        sold = "Verkocht" in (property_obj.tags or '') or "Verhuurd" in (property_obj.tags or '')
        
        properties_data.append({
            'name': property_obj.name,
            'type': property_obj.type,
            'postal_code_number': property_obj.postal_code_number,
            'postal_code_letters': property_obj.postal_code_letters,
            'city': property_obj.city,
            'province': province,
            'gemeente': gemeente,
            'price': property_obj.price,
            'area': property_obj.area,
            'num_of_rooms': property_obj.num_of_rooms,
            'energy_rating': property_obj.energy_rating,
            'url': property_obj.url,
            'estate_agent': property_obj.estate_agent,
            'tags': property_obj.tags or '',
            'sold': sold,
            'creation_date': datetime.date.today(),
            'mutation_date': datetime.date.today()
        })
    
    # Use PostgreSQL's INSERT ... ON CONFLICT for upsert
    from sqlalchemy.dialects.postgresql import insert
    
    stmt = insert(ScrapedProperty).values(properties_data)
    stmt = stmt.on_conflict_do_update(
        constraint='scraped_properties_unique',
        set_={
            'price': stmt.excluded.price,
            'area': stmt.excluded.area,
            'num_of_rooms': stmt.excluded.num_of_rooms,
            'energy_rating': stmt.excluded.energy_rating,
            'url': stmt.excluded.url,
            'estate_agent': stmt.excluded.estate_agent,
            'tags': stmt.excluded.tags,
            'sold': stmt.excluded.sold,
            'mutation_date': stmt.excluded.mutation_date
        }
    )
    
    session.execute(stmt)
    session.commit()
    session.close()
    
    print(f"Bulk database update completed at {datetime.datetime.now()}")
    logging.info(f'Bulk database update completed.')
    
except Exception as e:
    if 'session' in locals():
        session.rollback()
        session.close()
    print(f"Error during bulk database update: {e}")
    logging.error(f'Error during bulk database update: {e}')