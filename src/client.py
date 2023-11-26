import requests
import matplotlib.pyplot as plt



URL = "http://localhost:8000/get_distributio"

def display_disribution(settlement:str):
    param = {"settlement":settlement}
    response = requests.get(URL, params= param)
    if response.status_code == 200:
        response_lst = response.json()
        hour_df = [d['hour'] for d in response_lst]
        count_df = [d['count'] for d in response_lst]
        
        # Create a bar chart
        plt.bar(hour_df, count_df, color='#E84D88')
        plt.xlabel('Hour')
        plt.ylabel('Alerts Amount')
        plt.title(f'Alerts Hourly Distribution in {settlement}')
        plt.show()
    else: 
        raise Exception(f"An Error occures: status code- {response.status_code}")

display_disribution("sderot")