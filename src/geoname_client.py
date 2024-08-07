import warnings
warnings.filterwarnings('ignore')
import logging
import requests
import os
from dotenv import load_dotenv
load_dotenv()
GEONAMES_URL = os.getenv('GEONAMES_URL')
USERNAME_GEONAMES = os.getenv('USERNAME_GEONAMES')


class GeonameClient:

    # This function use geonames api and return True if the user settlement input is actually exist else returns False
    @staticmethod
    def is_real_settlement(settlement: str) -> bool:
        params = {
                    "name":settlement,
                    "country": "IL",
                    "maxRows":1, 
                    "username" : USERNAME_GEONAMES,
                    "fcode":"PPL" 
                }
        
        response = requests.get(GEONAMES_URL, params=params)
        if response.status_code == 200:    
            if response.json()['totalResultsCount'] == 0:
                logging.debug(f'GEONAME request: {response.text}')
                return False
            else:
                logging.debug(f'GEONAME request: {response.text}')
                return True
        else:
            raise requests.exceptions.RequestException("Error occurred while reaching to GeoNames API")