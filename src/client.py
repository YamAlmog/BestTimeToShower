import requests
import matplotlib.pyplot as plt
import argparse
from errors import WrongSettlementException, InvalidSettlement
from datetime import datetime
import threading


def fetch_alert_distribution(settlement:str, host:str, port:str, endpoint: str):
    param = {"settlement":settlement}
    url = f"http://{host}:{port}/{endpoint}"
    
    response = requests.get(url, params= param)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise response.raise_for_status()
    


def display_hourly_alerts_distribution(settlement:str, host:str, port:str):
    try:    
        response = fetch_alert_distribution(settlement, host, port, "get_hourly_distribution")

        if "message" in response:
            sentence = response.get('message', '')
            print(sentence)
        else:    
            response_list = response.json()
            hour_vector = [d['hour'] for d in response_list]
            alerts_count_vector = [d['count'] for d in response_list]
            # Create a bar chart
            plt.bar(hour_vector, alerts_count_vector, color='#E84D88')
            # Rotate x-axis labels vertically
            plt.xticks(rotation='vertical')
            plt.xlabel('Hour')
            plt.ylabel('Alerts Amount')
            plt.title(f'Alerts Hourly Distribution in {settlement}')
            plt.show()
    except requests.exceptions.HTTPError as ex:
        raise requests.exceptions.HTTPError(ex)
    
def display_daily_alerts_distribution(settlement:str, host:str, port:str):
    try:    
        response = fetch_alert_distribution(settlement, host, port, "get_distribution_per_day")

        if "message" in response:
            sentence = response.get('message', '')
            print(sentence)
        else:    
            date_vector = [datetime.strptime(d['date'], '%Y-%m-%d').date() for d in response]
            alerts_count_vector = [d['count'] for d in response]
            
            # Create a bar chart
            plt.bar(date_vector, alerts_count_vector, color='#E84D88')
            # Rotate x-axis labels vertically
            plt.xticks(rotation='vertical')
            plt.xlabel('Date')
            plt.ylabel('Alerts Amount')
            plt.title(f'Alerts Daily Distribution in {settlement}')
            plt.show()
    except requests.exceptions.HTTPError as ex:
        raise requests.exceptions.HTTPError(ex)


def main():
    try:    
        parser = argparse.ArgumentParser(description='Client for displaying alerts distribution.')
        parser.add_argument('hostname', type=str, help='The hostname of the server')
        parser.add_argument('port', type=str, help='The port of the server')

        args = parser.parse_args()

        HOST = args.hostname
        PORT = args.port

        while True:    
            settlement = input("Please input a settlement here --> ")
            if settlement == "exit":
                break
            display_daily_alerts_distribution(settlement, HOST, PORT) 
            display_hourly_alerts_distribution(settlement, HOST, PORT)

    except ValueError as ex:
        print(ex)
    except WrongSettlementException as ex:
        print(ex)
    except requests.exceptions.RequestException as ex:
        print(ex)
    except requests.exceptions.HTTPError as ex:
        print(ex) 
    except Exception as ex:
        print(f"Error getting data from host {ex}") 

if __name__ == "__main__":
    main()