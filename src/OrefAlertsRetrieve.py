import pandas as pd
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')
import requests
import json
from datetime import datetime, timedelta
import time 
from Errors import RetrieveDataException
import os
from dotenv import load_dotenv, dotenv_values
load_dotenv()
URL = os.getenv('ALERTS_URL')
ALERTS_DATA_FILE = os.getenv('ALERTS_DATA_FILE')
DAYS_INTERVAL = 2
SLEEP_TIME = 5


class OrefAlertsRetrieveData:
    def get_request(current_date, dest_date, alerts_list):
        params = {'lang':'en',
                    'fromDate': current_date,
                    'toDate': dest_date,
                    'mode': '0'}
        response = requests.get(URL, params=params)
        if response.status_code == 200:
            time.sleep(SLEEP_TIME)
            current_alerts_list = json.loads(response.text)
            missiles_alerts_list = [item for item in current_alerts_list if item['category_desc']=='Missiles']
            alerts_list += missiles_alerts_list
        else:
            raise RetrieveDataException("HTTP error occurred with get request alerts data")
            

    def get_oref_alarms(self, from_date, to_date):
        try:    
            all_time_alarams_list = []
            current_date = datetime.strptime(from_date, '%d.%m.%y')
            target_date = datetime.strptime(to_date, '%d.%m.%y')
            print(current_date)
            while current_date <= target_date:
                dest_date = current_date + timedelta(days=DAYS_INTERVAL)  
                self.get_request(current_date, dest_date, all_time_alarams_list)
                print(f"Get the alarms from date: {current_date} to date: {dest_date}")
                current_date += timedelta(days=DAYS_INTERVAL)
            
            difference = current_date - target_date
            days_diff = difference.days
            day_to_add = DAYS_INTERVAL-days_diff
            current_date -= timedelta(days=DAYS_INTERVAL) # return current_date to the correct date cause we add DAYS_INTERVAL at the end of while loop
            self.get_request(current_date, current_date + timedelta(days=day_to_add), all_time_alarams_list)
            
            df = pd.DataFrame(all_time_alarams_list)
            # Convert the data column to lowercase to make it easy to work with the dataframe in future
            df['data'] = df['data'].str.lower()
            df.to_csv(ALERTS_DATA_FILE)
        except RetrieveDataException as ex:
            raise ex
        except PermissionError as e:
            raise (f"An error occurwith csv file:{e}")