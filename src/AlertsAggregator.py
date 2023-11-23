import pandas as pd
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')



class AlertsAggregator:
    def __init__(self, df):
        self.df = df

    # This func get dataframe and return a set of all the settlement in df
    def return_data_list(self):
        items_lst = self.df['data'].tolist()
        items_lst_no_duplications = set(items_lst)
        return items_lst_no_duplications


    # This function get dataframe and settlement_name and return a list of all the settlement in the df that settlement_name is insid them
    def create_user_settl_lst(self, settlement):
        given_lst = self.return_data_list()
        settl_lst = []
        for item in given_lst:
            if settlement.lower() in item:
                if item not in settl_lst:
                    settl_lst.append(item)
        return settl_lst
    


    # This function display the distribution of alarms in a specific settlement
    def display_dist(self, settlement):
        settlement_lst= self.create_user_settl_lst(settlement)
        
        if settlement_lst != []:
            # Filtered the df by the given settlement lst
            filtered_df = self.df[self.df['data'].isin(settlement_lst)]
            
            # Access the 'time' column of the DataFrame
            filtered_df['time'] = pd.to_datetime(filtered_df['time'])

            # Plot histogram
            plt.hist(filtered_df['time'].dt.hour + filtered_df['time'].dt.minute / 60, bins=4 * 24, range=(0, 24), color='skyblue', edgecolor='black')

            plt.xlabel('Time')
            plt.ylabel('Alarms')
            plt.title(f"Distribution of alarms in {settlement}")
            # Set X-axis ticks and labels
            plt.xticks(range(24), labels=[str(i) for i in range(24)])
            plt.show()

        else:
            raise ValueError("You selected a Settlement that does not exist.")
        

    # This function receives df and settlement name then count the total amount of alerts in this settlement
    def alerts_count(self, settlement):
        user_settlement_lst= self.create_user_settl_lst(settlement)
        if user_settlement_lst != []:
            # Filtered the df by the given settlement lst
            filtered_df = self.df[self.df['data'].isin(user_settlement_lst)]
            return filtered_df.shape[0]
        else:
            raise ValueError("You selected a Settlement that does not exist.")
        

    # This function receives df and count the total amount of alerts in the requested date range
    def total_alerts_count(self):
        total_alerts = self.df.shape[0]
        return total_alerts
    
    
    # This function receives city, and range of time and returns the distribution of the alarms in quarters of an hour 
    def create_quarter_hour_column(self, settlement, start_time, end_time):
        settlement_lst= self.create_user_settl_lst(settlement)
        
        if settlement_lst != []:
            filtered_df = self.df[self.df['data'].isin(settlement_lst)]
            filtered_df['time'] = pd.to_datetime(filtered_df['time'])
            
            # Filter the deteframe by the start and end time of the user
            filtered_df = filtered_df[(filtered_df['time'].dt.hour >= start_time) & (filtered_df['time'].dt.hour < end_time)]
            
            # Create new column which represent the time of the alarm among 15 minuts 
            filtered_df['quarter_hour'] = filtered_df['time'].dt.minute // 15
            quarter_hour_counts = filtered_df['quarter_hour'].value_counts().reindex([0,1,2,3], fill_value=0)
            print(quarter_hour_counts)
            return quarter_hour_counts
        
        else:
            raise ValueError("You selected a Settlement that does not exist.")

    def best_time_to_shower(self, settlement, start_time, end_time):
        quarter_hour_counts = self.create_quarter_hour_column(settlement, start_time, end_time)
        # Detect the quarter of an hour that appeared the less
        best_time = quarter_hour_counts.idxmin()
        return (f"The best time to take a shower is at the quarter: {best_time}")
    

    def worst_time_to_shower(self, settlement, start_time, end_time):
        quarter_hour_counts = self.create_quarter_hour_column(settlement, start_time, end_time)
        # Detect the quarter of an hour that appeared the most
        worst_time = quarter_hour_counts.idxmax()
        return (f"The worst time to take a shower is at the quarter: {worst_time}")
    

    def poorest_city(self):
        city_alerts_count = self.df['data'].value_counts()
        poorest_city = city_alerts_count.idxmax()
        return (f"The city that suffers the most from alarms is:{poorest_city}")