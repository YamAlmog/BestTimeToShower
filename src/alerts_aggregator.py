import pandas as pd
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')
from models import AlertCountPerDay, AlertType, AlertCountPerHour
from errors import WrongSettlementException, NoAlarmsException, InvalidSettlement
from geoname_client import GeonameClient
from datetime import time, datetime, timedelta
import requests
import os
from dotenv import load_dotenv, dotenv_values
load_dotenv()
import logging

logging.basicConfig(filename='app.log', filemode='w',  level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')



class AlertsAggregator:
    def __init__(self, df):
        self.df = df

    # This function get dataframe and return all the settlement in it
    def all_settlements_in_oref_alerts_df(self) -> list:
        items_list = self.df['settlement'].tolist()
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



    # This function display the distribution of alarms in a specific settlement
    def get_alerts_distribution(self, settlement: str) -> list:
        try:
            settlement_list= self.create_user_settlement_list(settlement)
            is_settlement_exist = GeonameClient.is_real_settlement(settlement)
            if settlement_list != []:
                # Filtered the df by the given settlement list
                filtered_df = self.df[self.df['settlement'].isin(settlement_list)]
                
                # Access the 'time' column of the dataframe
                filtered_df['time'] = pd.to_datetime(filtered_df['time'])
                filtered_df['hours'] = filtered_df['time'].dt.strftime("%H")
                # use list comprehension to create list of two characters wide str hours like this ["00", "01",... ,"23"]
                hour_list = [f'{i:02}' for i in range(24)]
                alert_distribution = filtered_df['hours'].value_counts().reindex(hour_list, fill_value=0)
                alert_distribution = alert_distribution.to_dict()
                distribution_list = []
                for i in hour_list:
                    alert_count_obj = AlertCountPerHour(hour=f"{i}:00", count=alert_distribution[i])
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
       
  

    def get_alerts_distribution_per_day(self, settlement: str) -> list:
        try:
            settlement_list = self.create_user_settlement_list(settlement)
            is_settlement_exist = GeonameClient.is_real_settlement(settlement)
            if settlement_list:
                # Filter the DataFrame by the given settlement list
                filtered_df = self.df[self.df['settlement'].isin(settlement_list)]
                
                # Access the 'date' column of the DataFrame
                filtered_df['date'] = pd.to_datetime(filtered_df['date'], format='%d.%m.%Y')
                
                # Group by 'date' and count the occurrences
                alert_distribution = filtered_df['date'].value_counts().sort_index()
                alert_distribution = alert_distribution.to_dict()
                
                distribution_list = []
                for date, count in alert_distribution.items():
                    alert_count_obj = AlertCountPerDay(date=date.strftime('%Y-%m-%d'), count=count)
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
    def alerts_count(self, settlement: str) -> int:
        try:
            user_settlement_list= self.create_user_settlement_list(settlement)
            is_settlement_exist = GeonameClient.is_real_settlement(settlement)
            
            if user_settlement_list != []:
                # Filtered the df by the given settlement list
                filtered_df = self.df[self.df['settlement'].isin(user_settlement_list)]
                alert_amount = filtered_df.shape[0]
                return alert_amount
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
    def total_alerts_count(self) -> int:
        total_alerts = self.df.shape[0]
        return total_alerts
    

    # This function get range of time from user and returns list of time in quarter hour interval between this range 
    def quarter_hour_intervals_list(self, start_time : time, end_time :time) -> list:
        time_list = []
        current_time = time(start_time.hour, 0)
       
        while current_time <= end_time:
            time_list.append(current_time.strftime('%H:%M'))
            # i convert the current time to datetime object for adding 15 minutes correctly
            current_time = (datetime.combine(datetime.today(), current_time) + timedelta(minutes=15)).time()

        return time_list
    
    # This function receives city, and range of time and returns pd series of time and its count value
    def create_adjusted_time_column(self, settlement :str, start_time : time, end_time :time):
        try:    
            settlement_list = self.create_user_settlement_list(settlement)
            is_settlement_exist = GeonameClient.is_real_settlement(settlement)
            # Extract the hour
            start_hour = start_time.hour
            end_hour = end_time.hour
            missiles_df = self.df[self.df['alert_type'] == 'Missiles']
            
            if settlement_list != []:
                filtered_df = missiles_df[missiles_df['settlement'].isin(settlement_list)]
            
                filtered_df['time'] = pd.to_datetime(filtered_df['time']) 
                # Filter the dataframe by the start and end time of the user
                filtered_df = filtered_df[(filtered_df['time'].dt.hour >= start_hour) & (filtered_df['time'].dt.hour < end_hour)]
                
                # Create new column which represent the minutes by the nearest 15-minute interval
                filtered_df['adjusted_time'] = filtered_df['time'].dt.strftime('%H:%M')
                filtered_df['adjusted_time'] = pd.to_datetime(filtered_df['adjusted_time']).dt.round('15min').dt.strftime('%H:%M')
                logging.debug(f'This is a quick look of how the filtered df seen: {filtered_df}')
                
                # Create time list of 15-minute interval and use it to map the amount of alerts for every 15-minute
                time_list = self.quarter_hour_intervals_list(start_time, end_time)
                logging.debug(f'This is the 15-minute interval time list: {time_list}')
                adjusted_time_counts = filtered_df['adjusted_time'].value_counts().reindex(time_list, fill_value=0)
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
    
        

    # This function use create_quarter_hour_column func to return the best time to shower in
    def best_time_to_shower(self, settlement : str, start_time : time, end_time : time) -> str:
        try:    
            # the function create_adjusted_time_column returns pd series of time object and its count value 
            adjusted_time_counts = self.create_adjusted_time_column(settlement, start_time, end_time)
            logging.debug(f'This is the alert distribution by 15-minute interval in {settlement}: \n{adjusted_time_counts}')
            
            # Detect the quarter of an hour that appeared the less-it will be the best time to shower
            best_time = adjusted_time_counts.idxmin()
            return best_time
            
        except NoAlarmsException as ex:
            raise NoAlarmsException(ex) 
        except InvalidSettlement as ex:
            raise InvalidSettlement(ex)
        except requests.exceptions.RequestException as ex:
            raise requests.exceptions.RequestException(ex)
   
    
    # This function use create_quarter_hour_column func to return the worst time to shower in
    def worst_time_to_shower(self, settlement: str, start_time: time, end_time: time) -> str:
        try:    
            # the function create_adjusted_time_column returns pd series of time object and its count value 
            adjusted_time_counts = self.create_adjusted_time_column(settlement, start_time, end_time)
            logging.debug(f'This is the alert distribution by 15-minute interval in {settlement}: \n{adjusted_time_counts}')
            
            # Detect the quarter of an hour that appeared the most- it will be the worst time to shower
            worst_time = adjusted_time_counts.idxmax()
            return worst_time
            
        except NoAlarmsException as ex:
            raise NoAlarmsException(ex) 
        except InvalidSettlement as ex:
            raise InvalidSettlement(ex)
        except requests.exceptions.RequestException as ex:
            raise requests.exceptions.RequestException(ex)
      

    # This function return a list of all the settlements in the data column in the dataframe
    def retrieve_all_settlement(self) -> list:
        settlement_df = self.df.drop_duplicates(subset='settlement')
        settlement_list = settlement_df['settlement'].tolist()
        return settlement_list


    # This function return the area that suffer the most from alerts use enum for alert type
    def most_hitted_settlement(self, alert_type:AlertType):
        hostile_aircraft_df = self.df[self.df['alert_type'] == alert_type.value]
        city_alerts_count = hostile_aircraft_df['settlement'].value_counts()
        most_hitted_city = city_alerts_count.idxmax()
        return most_hitted_city


    def reload_data(self, new_df):
        self.df = new_df


    def  get_all_alerts_types(self):
        unique_alert_types = self.df['alert_type'].unique()
        return unique_alert_types