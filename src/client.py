import requests
import matplotlib.pyplot as plt
import argparse
from errors import WrongSettlementException, InvalidSettlement
from datetime import datetime
from matplotlib.dates import DateFormatter


def fetch_alert_distribution(settlement:str, host:str, port:str, endpoint: str):
    param = {"settlement":settlement}
    url = f"http://{host}:{port}/{endpoint}"
    
    response = requests.get(url, params= param)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise response.raise_for_status()
    

def get_hourly_alerts_distribution(settlement:str, host:str, port:str):
    try:    
        response = fetch_alert_distribution(settlement, host, port, "get_hourly_distribution")
        if "message" in response:
            sentence = response.get('message', '')
            print(sentence)
        else:    
            hour_vector = [d['hour'] for d in response]
            alerts_count_vector = [d['count'] for d in response]
            return hour_vector, alerts_count_vector
    except requests.exceptions.HTTPError as ex:
        raise requests.exceptions.HTTPError(ex)
    
def get_daily_alerts_distribution(settlement:str, host:str, port:str):
    try:    
        response = fetch_alert_distribution(settlement, host, port, "get_distribution_per_day")
        if "message" in response:
            sentence = response.get('message', '')
            print(sentence)
        else:    
            date_vector = [datetime.strptime(d['date'], '%Y-%m-%d').date() for d in response]
            alerts_count_vector = [d['count'] for d in response]
            return date_vector, alerts_count_vector
    except requests.exceptions.HTTPError as ex:
        raise requests.exceptions.HTTPError(ex)


def display_alerts_distribution(settlement: str, host: str, port: str):
    daily_dates, daily_counts = get_daily_alerts_distribution(settlement, host, port)
    hourly_hours, hourly_counts = get_hourly_alerts_distribution(settlement, host, port)

    fig, axs = plt.subplots(2, 1, figsize=(12, 12))
    
    if daily_dates and daily_counts:
        axs[0].bar(daily_dates, daily_counts, color='#E84D88')
        axs[0].set_xticks(daily_dates)
        axs[0].set_xticklabels(daily_dates, rotation='vertical')
        date_format = DateFormatter("%b %d")
        axs[0].xaxis.set_major_formatter(date_format)
        axs[0].set_xlabel('Date')
        axs[0].set_ylabel('Alerts Amount')
        axs[0].set_title(f'Alerts Daily Distribution in {settlement}')
    
    if hourly_hours and hourly_counts:
        axs[1].bar(hourly_hours, hourly_counts, color='#4E84E8')
        axs[1].set_xticks(hourly_hours)
        axs[1].set_xticklabels(hourly_hours, rotation='vertical')
        axs[1].set_xlabel('Hour')
        axs[1].set_ylabel('Alerts Amount')
        axs[1].set_title(f'Alerts Hourly Distribution in {settlement}')
    
    plt.tight_layout()
    plt.show()

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

            display_alerts_distribution(settlement, HOST, PORT)

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