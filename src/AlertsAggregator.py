import pandas as pd
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')
from datetime import datetime
from models import AlertCountPerDay
from Errors import WrongSettlementException, NoAlarmsException, InvalidSettlement
from datetime import time, datetime, timedelta
import requests
import os
from dotenv import load_dotenv, dotenv_values
load_dotenv()
GEONAMES_URL = os.getenv('GEONAMES_URL')



class AlertsAggregator:
    def __init__(self, df):
        self.df = df

    # This function get dataframe and return all the settlement in it
    def all_settlements_in_oref_alerts_df(self) -> list:
        items_list = self.df['data'].tolist()
        items_list_no_duplications = set(items_list)
        return items_list_no_duplications


    # This function get dataframe and settlement_name and return a list of settlement which the settlement name is found
    def create_user_settlement_list(self, settlement: str) -> list:
        given_list = self.all_settlements_in_oref_alerts_df()
        settlement_list = []
        for item in given_list:
            if settlement.lower() == item or f"{settlement.lower()}, " in item:
                return [item]
            elif f"{settlement.lower()} " in item  or f" {settlement.lower()}" in item or settlement.lower() in item :
                if item not in settlement_list:
                    settlement_list.append(item)
        if settlement_list != []:
            raise WrongSettlementException(f"Incorrect settlement, please try again input one of this options: {settlement_list}")
        return settlement_list

    
    # This function use geonames api and return True if the user settlement input is actually exist else returns False
    def does_the_settlement_exist(self, settlement: str) -> bool:
        params = {
                  "name":settlement,
                  "country": "IL",
                  "maxRows":1, 
                  # i try os.getenv('USERNAME_GEONAMES') but no success
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
            raise requests.exceptions.RequestException("An error occurred while try to use get request from GeoNames api")


    # This function display the distribution of alarms in a specific settlement
    def get_alerts_distribution(self, settlement: str) -> list:
        try:
            settlement_list= self.create_user_settlement_list(settlement)
            is_settlement_exist = self.does_the_settlement_exist(settlement)
            if settlement_list != []:
                # Filtered the df by the given settlement list
                filtered_df = self.df[self.df['data'].isin(settlement_list)]
                
                # Access the 'time' column of the dataframe
                filtered_df['time'] = pd.to_datetime(filtered_df['time'])
                filtered_df['hours'] = filtered_df['time'].dt.strftime("%H")
                hour_list = ["00","01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23"]
                alert_distribution = filtered_df['hours'].value_counts().reindex(hour_list, fill_value=0)
                alert_distribution = alert_distribution.to_dict()
                distribution_list = []
                for i in hour_list:
                    alert_count_obj = AlertCountPerDay(hour=i, count=alert_distribution[i])
                    distribution_list.append(alert_count_obj)
                return distribution_list
            elif is_settlement_exist:
                raise NoAlarmsException(f"There were no alarms in the settlement: {settlement}")   
            else:
                raise InvalidSettlement("You selected a Settlement that does not exist.")
        
        except NoAlarmsException as ex:
            raise NoAlarmsException(ex)   

        except InvalidSettlement as ex:
            raise InvalidSettlement(ex)   

        except requests.exceptions.RequestException as ex:
            raise requests.exceptions.RequestException(ex)
        
        except WrongSettlementException as ex:
            raise WrongSettlementException(ex)
       
        
        
        

    # This function receives df and settlement name then count the total amount of alerts in this settlement
    def alerts_count(self, settlement: str) -> str:
        try:
            user_settlement_list= self.create_user_settlement_list(settlement)

            is_settlement_exist = self.does_the_settlement_exist(settlement)
            print(is_settlement_exist)
            if user_settlement_list != []:
                # Filtered the df by the given settlement list
                filtered_df = self.df[self.df['data'].isin(user_settlement_list)]
                alert_amount = filtered_df.shape[0]
                return {"message" : f"The total amount of alerts in {settlement} is: {alert_amount}"}
            elif is_settlement_exist:
                raise NoAlarmsException(f"There were no alarms in the settlement: {settlement}")
            else:
                raise InvalidSettlement("You selected a Settlement that does not exist.")
        
        except NoAlarmsException as ex:
            raise NoAlarmsException(ex)   
        except InvalidSettlement as ex:
            raise InvalidSettlement(ex)       
        except WrongSettlementException as ex:
            raise WrongSettlementException(ex)
        except requests.exceptions.RequestException as ex:
            raise requests.exceptions.RequestException(ex)

        
    # This function receives df and count the total amount of alerts in the requested date range
    def total_alerts_count(self) -> str:
        total_alerts = self.df.shape[0]
        return {"message" : f"The total amount of alerts in our country is: {total_alerts}"}
    
    def quarter_hour_intervals_list(self, start_time : time, end_time :time) -> list:
        time_list = []
        current_time = time(start_time.hour, 0)
        print(current_time)
        while current_time <= end_time:
            time_list.append(current_time.strftime('%H:%M'))
            current_time = (datetime.combine(datetime.today(), current_time) + timedelta(minutes=15)).time()

        return time_list
    
    # This function receives city, and range of time and returns a df distribution of the alarms in quarters of an hour 0(0-15) 1(15-30) 2(30-45) 3(45-0)
    def create_adjusted_time_column(self, settlement :str, start_time : time, end_time :time):
        try:    
            settlement_list= self.create_user_settlement_list(settlement)
            is_settlement_exist = self.does_the_settlement_exist(settlement)
            # Extract the hour
            start_hour = start_time.hour
            end_hour = end_time.hour

            if settlement_list != []:
                filtered_df = self.df[self.df['data'].isin(settlement_list)]
                filtered_df['time'] = pd.to_datetime(filtered_df['time']) 
                # Filter the dataframe by the start and end time of the user
                filtered_df = filtered_df[(filtered_df['time'].dt.hour >= start_hour) & (filtered_df['time'].dt.hour < end_hour)]
                
                # Create new column which represent the minutes by the nearest 15-minute interval
                filtered_df['adjusted_time'] = filtered_df['time'].dt.strftime('%H:%M')
                filtered_df['adjusted_time'] = pd.to_datetime(filtered_df['adjusted_time']).dt.round('15min').dt.strftime('%H:%M')
                print(filtered_df)
                
                time_list = self.quarter_hour_intervals_list(start_time, end_time)
                print(time_list)
                adjusted_time_counts = filtered_df['adjusted_time'].value_counts().reindex(time_list, fill_value=0)
                print(adjusted_time_counts)
                return adjusted_time_counts
            elif is_settlement_exist:
                raise NoAlarmsException(f"There were no alarms in the settlement: {settlement}")   
            else:
                raise InvalidSettlement("You selected a Settlement that does not exist.")
        
        except NoAlarmsException as ex:
            raise NoAlarmsException(ex)   
        except InvalidSettlement as ex:
            raise InvalidSettlement(ex)
        except WrongSettlementException as ex:
            raise WrongSettlementException(ex)
        except requests.exceptions.RequestException as ex:
            raise requests.exceptions.RequestException(ex)
    
        

    # This function use create_quarter_hour_column func to return the best quarter of an hour to shower in
    def best_time_to_shower(self, settlement : str, start_time : time, end_time : time) -> str:
        try:    
            adjusted_time_counts = self.create_adjusted_time_column(settlement, start_time, end_time)
            print(f"{settlement} \n{adjusted_time_counts}")
            
            # Detect the quarter of an hour that appeared the less
            best_time = adjusted_time_counts.idxmin()
            return {"message" : f"The best time to take a shower is at: {best_time}"}
            
        except NoAlarmsException as ex:
            raise NoAlarmsException(ex) 
        except InvalidSettlement as ex:
            raise InvalidSettlement(ex)
        except requests.exceptions.RequestException as ex:
            raise requests.exceptions.RequestException(ex)
   
    
    # This function use create_quarter_hour_column func to return the worst quarter of an hour to shower in
    def worst_time_to_shower(self, settlement: str, start_time: time, end_time: time) -> str:
        try:    
            adjusted_time_counts = self.create_adjusted_time_column(settlement, start_time, end_time)
            print(f"{settlement} \n{adjusted_time_counts}")
            
            # Detect the quarter of an hour that appeared the less
            worst_time = adjusted_time_counts.idxmax()
            return {"message" : f"The worst time to take a shower is at: {worst_time}"}
            
        except NoAlarmsException as ex:
            raise NoAlarmsException(ex) 
        except InvalidSettlement as ex:
            raise InvalidSettlement(ex)
        except requests.exceptions.RequestException as ex:
            raise requests.exceptions.RequestException(ex)
      

    # This function return the area that suffer the most from alerts
    def poorest_area(self) -> str:
        city_alerts_count = self.df['data'].value_counts()
        poorest_city = city_alerts_count.idxmax()
        return {"message":f"The area that suffers the most from alarms is: {poorest_city}"}
    
    # This function return a list of all the settlements in the data column in the dataframe
    def retrieve_all_settlement(self) -> str:
        settlement_df = self.df.drop_duplicates(subset='data')
        settlement_list = settlement_df['data'].tolist()
        return {"message": f"The settlements in the df are: {settlement_list}"}
