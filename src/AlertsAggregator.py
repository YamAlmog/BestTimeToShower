import pandas as pd
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')
from datetime import datetime
from models import AlertCountPerDay
from datetime import time, datetime
import requests
GEONAMES_URL = "http://api.geonames.org/searchJSON"


class AlertsAggregator:
    def __init__(self, df):
        self.df = df

    # This func get dataframe and return a set of all the settlement in df
    def return_data_list(self):
        items_lst = self.df['data'].tolist()
        items_lst_no_duplications = set(items_lst)
        return items_lst_no_duplications


    # This function get dataframe and settlement_name and return a list of all the settlement in the df that settlement_name is insid them
    def create_user_settl_lst(self, settlement: str):
        given_lst = self.return_data_list()
        settl_lst = []
        for item in given_lst:
            if f"{settlement.lower()} " in item or f"{settlement.lower()}, " in item or f" {settlement.lower()}" in item or settlement.lower() == item:
                if item not in settl_lst:
                    settl_lst.append(item)
        return settl_lst
    
    
    # This function use geonames api and return True if the user settlement input is actually exist else returns False
    def does_the_settlement_exist(self, settlement: str):
        params = {
                  "name":settlement,
                  "country": "IL",
                  "maxRows":1, 
                  "username":"roit1",
                  "fcode":"PPL" 
                }
        print(params)
        response = requests.get(GEONAMES_URL, params=params)
        if response.status_code == 200:    
            if response.json()['totalResultsCount'] == 0:
                print(response.text)
                return False
            else:
                print(response.text)
                return True
        else:
            raise requests.exceptions.RequestException("An error occure while try to use get request from GeoNames api")


    # This function display the distribution of alarms in a specific settlement
    def display_dist(self, settlement: str):
        settlement_lst= self.create_user_settl_lst(settlement)
        try:
            is_settlement_exist = self.does_the_settlement_exist(settlement)
            if settlement_lst != []:
                # Filtered the df by the given settlement lst
                filtered_df = self.df[self.df['data'].isin(settlement_lst)]
                
                # Access the 'time' column of the DataFrame
                filtered_df['time'] = pd.to_datetime(filtered_df['time'])
                filtered_df['hours'] = filtered_df['time'].dt.strftime("%H")
                hour_lst = ["00","01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23"]
                alert_distribution = filtered_df['hours'].value_counts().reindex(hour_lst, fill_value=0)
                alert_distribution = alert_distribution.to_dict()
                distribution_lst = []
                for i in hour_lst:
                    alert_count_obj = AlertCountPerDay(hour=i, count=alert_distribution[i])
                    distribution_lst.append(alert_count_obj)
                return distribution_lst
            elif is_settlement_exist:
                return {"message" : f"There were no alarms in the settlement: {settlement}"}   
            else:
                raise ValueError("You selected a Settlement that does not exist.")
        
        except requests.exceptions.RequestException as req_err:
            raise requests.exceptions.RequestException(f"Request error occurred: {req_err}")
        
        except ValueError as val_err:
            raise ValueError(f"ValueError error occurred:{val_err}")    


    # This function receives df and settlement name then count the total amount of alerts in this settlement
    def alerts_count(self, settlement: str):
        try:
            user_settlement_lst= self.create_user_settl_lst(settlement)

            is_settlement_exist = self.does_the_settlement_exist(settlement)
            print(is_settlement_exist)
            if user_settlement_lst != []:
                # Filtered the df by the given settlement lst
                filtered_df = self.df[self.df['data'].isin(user_settlement_lst)]
                alert_amount = filtered_df.shape[0]
                return {"message" : f"The total amount of alerts in {settlement} is: {alert_amount}"}
            elif is_settlement_exist:
                return {"message" : f"There were no alarms in the settlement: {settlement}"}
            else:
                raise ValueError("You selected a Settlement that does not exist.")
        
        except requests.exceptions.RequestException as req_err:
            raise requests.exceptions.RequestException(f"Request error occurred: {req_err}")
        
        except ValueError as val_err:
            raise ValueError(f"ValueError error occurred:{val_err}")

        

    # This function receives df and count the total amount of alerts in the requested date range
    def total_alerts_count(self):
        total_alerts = self.df.shape[0]
        return {"message" : f"The total amount of alerts in our country is: {total_alerts}"}
    
   

    # This function receives city, and range of time and returns a df distribution of the alarms in quarters of an hour 0(0-15) 1(15-30) 2(30-45) 3(45-0)
    def create_quarter_hour_column(self, settlement :str, start_time : time, end_time :time):
        settlement_lst= self.create_user_settl_lst(settlement)
        is_settlement_exist = self.does_the_settlement_exist(settlement)
        # Extract the hour
        start_hour = start_time.hour
        end_hour = end_time.hour

        if settlement_lst != []:
            filtered_df = self.df[self.df['data'].isin(settlement_lst)]
            filtered_df['time'] = pd.to_datetime(filtered_df['time']) 
            # Filter the deteframe by the start and end time of the user
            filtered_df = filtered_df[(filtered_df['time'].dt.hour >= start_hour) & (filtered_df['time'].dt.hour < end_hour)]
            # Create new column which represent the time of the alarm among 15 minuts 
            filtered_df['quarter_hour'] = filtered_df['time'].dt.minute // 15
            quarter_hour_counts = filtered_df['quarter_hour'].value_counts().reindex([0,1,2,3], fill_value=0)
            return quarter_hour_counts
        elif is_settlement_exist:
            return {"message" : f"There were no alarms in the settlement: {settlement}"}
        else:
            raise ValueError("You selected a Settlement that does not exist.")

    # This function use create_quarter_hour_column func to return the best quarter of an hour to shower in
    def best_time_to_shower(self, settlement : str, start_time : time, end_time : time):
        try:    
            quarter_hour_counts = self.create_quarter_hour_column(settlement, start_time, end_time)
            print(f"{settlement} \n{quarter_hour_counts}")
            if isinstance(quarter_hour_counts, pd.Series):
                # Detect the quarter of an hour that appeared the less
                best_time = quarter_hour_counts.idxmin()
                return {"mesage" : f"The best time to take a shower is at the quarter: {best_time}"}
            else:
                return quarter_hour_counts
        except ValueError as e:
            raise ValueError(f"Error: {e}")
        except requests.exceptions.RequestException as req_err:
            raise requests.exceptions.RequestException(f"Request error occurred: {req_err}")
        except Exception as ex:
            raise Exception(f"Error: {ex}")
    
    # This function use create_quarter_hour_column func to return the worst quarter of an hour to shower in
    def worst_time_to_shower(self, settlement: str, start_time: time, end_time: time):
        try:    
            quarter_hour_counts = self.create_quarter_hour_column(settlement, start_time, end_time)
            print(f"{settlement} \n{quarter_hour_counts}")
            if isinstance(quarter_hour_counts, pd.Series):
                # Detect the quarter of an hour that appeared the less
                worst_time = quarter_hour_counts.idxmax()
                return {"mesage" : f"The worst time to take a shower is at the quarter: {worst_time}"}
            else:
                return quarter_hour_counts
        except ValueError as e:
            raise ValueError(f"Error: {e}")
        except requests.exceptions.RequestException as req_err:
            raise requests.exceptions.RequestException(f"Request error occurred: {req_err}")
        except Exception as ex:
            raise Exception(f"Error: {ex}")

    # This function return the area that suffer the most from alerts
    def poorest_area(self):
        city_alerts_count = self.df['data'].value_counts()
        poorest_city = city_alerts_count.idxmax()
        return {"message":f"The area that suffers the most from alarms is: {poorest_city}"}
    
    # This function return a list of all the settlements in the data column in the datafram
    def retrieve_all_settlement(self):
        settlement_df = self.df.drop_duplicates(subset='data')
        settl_list = settlement_df['data'].tolist()
        return {"message": f"The settlements in the df are: {settl_list}"}
