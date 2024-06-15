import pandas as pd
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')
import requests
import json
from datetime import datetime, timedelta
import time 
from errors import OrefAPIException
import os
from dotenv import load_dotenv, dotenv_values
import logging

load_dotenv()
URL = os.getenv('ALERTS_URL')
ALERTS_DATA_FILE = os.getenv('ALERTS_DATA_FILE')
DAYS_INTERVAL = 2
SLEEP_TIME = 5


class OrefAlertsIndexer:
    # retrieve the alerts data documentation from oref alerts api for which the alert category is missiles
    def get_alerts_from_oref_api(self, current_date, dest_date):
        params = {'lang':'en',
                    'fromDate': current_date,
                    'toDate': dest_date,
                    'mode': '0'}
        response = requests.get(URL, params=params)

        if response.status_code == 200:
            time.sleep(SLEEP_TIME)
            current_alerts_list = json.loads(response.text)
            missiles_alerts_list = [item for item in current_alerts_list if item['category_desc']=='Missiles']
            return missiles_alerts_list 

            
        else:
            raise OrefAPIException("HTTP error occurred with get request alerts data")
            
    # filter the alerts data by range of time, arrange the data and insert it to csv file
    def arrange_alarms_within_csv(self, from_date: str, to_date: str):
        try:    
            all_time_alarams_list = []
            logging.debug(f'from {from_date} to {to_date}')
            current_date = datetime.strptime(from_date, '%d.%m.%Y')
            logging.debug(f'This is the current date time: {current_date}')
            target_date = datetime.strptime(to_date, '%d.%m.%Y')
            logging.debug(f'This is the target date time: {target_date}')
            
            while current_date <= target_date:
                dest_date = current_date + timedelta(days=DAYS_INTERVAL)  # i took the data from oref api 2 days interval because there is allot of it
                all_time_alarams_list += self.get_alerts_from_oref_api(current_date, dest_date)
                logging.debug(f"Get the alarms from date: {current_date} to date: {dest_date}")
                current_date += timedelta(days=DAYS_INTERVAL)
            
            difference = current_date - target_date
            days_diff = difference.days
            day_to_add = DAYS_INTERVAL-days_diff
            current_date -= timedelta(days=DAYS_INTERVAL) # return current_date to the correct date cause we add DAYS_INTERVAL at the end of while loop
            self.get_alerts_from_oref_api(current_date, current_date + timedelta(days=day_to_add))
            
            df = pd.DataFrame(all_time_alarams_list)
            # Convert the data column to lowercase to make it easy to work with the dataframe in future
            df['data'] = df['data'].str.lower()
            df.to_csv(ALERTS_DATA_FILE)
        except OrefAPIException as ex:
            raise ex
        except PermissionError as e:
            raise (f"An error occurred with csv file:{e}")
        