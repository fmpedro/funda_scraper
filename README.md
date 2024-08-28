This is a python script to scrape data from funda.nl

The main script is "funda_scraper.py". It generates a Sqlite database where the scrapped data is stored. This database can then be used as the basis for your custom data analysis, dashboarding (for example, using Metabase), etc.

The script is currently configured to scraped the results page from Funda, regarding houses or apartments, in any location in the Netherlands, listed on Funda in the last 5 days. I also run this scraper every night. This ensures that I capture all new listings, thus building up the database. You may adjust this to your specific needs, by changing the "base_url" string, as well as the frequency with which you run the script. Keep in mind that abusing the scrapping might get your IP temporarily or permanentely blocked by Funda. Also, funda result pages are capped at 666 pages, so if your query would return more results than this, the scrapper will not capture them all.

There is also a "sold_properties_updater.py" script, which is meant to periodically go through all the properties in the database that are marked as not sold (sold=0) and check the listing in funda, to update it's current status. I currently run this script once a month. Be aware that, depending on the amount of available properties in your database, this can be quite a list, so running this script too frequently might also cause Funda to block your IP.

