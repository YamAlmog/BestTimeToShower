import requests
import matplotlib.pyplot as plt
import sys
from Errors import WrongSettlementException



def display_disribution(settlement:str, host:str, port:str):
    param = {"settlement":settlement}
    url = f"http://{host}:{port}/get_distribution"
    
    response = requests.get(url, params= param)
    print(response.status_code)
    if response.status_code == 200:
        # in case the settlement is exist but no alarms were there
        if "message" in response.json():
            sentence = response.json().get('message', '')
            print(sentence)
        response_lst = response.json()
        hour_df = [d['hour'] for d in response_lst]
        count_df = [d['count'] for d in response_lst]
        # Create a bar chart
        plt.bar(hour_df, count_df, color='#E84D88')
        plt.xlabel('Hour')
        plt.ylabel('Alerts Amount')
        plt.title(f'Alerts Hourly Distribution in {settlement}')
        plt.show()
    elif response.status_code == 400:
        error_details = response.json().get('detail', '')
        raise ValueError(error_details)
    elif response.status_code == 401:
        error_details = response.json().get('detail', '')
        raise WrongSettlementException(error_details)
    elif response.status_code == 402:
        error_details = response.json().get('detail', '')
        raise requests.exceptions.RequestException(error_details)
    elif response.status_code == 500:
        error_details = response.json().get('detail', '')
        raise Exception(error_details)
    

def main():
    try:    
        print(len(sys.argv))
        if len(sys.argv) != 3:
            print("Usage: python client.py <hostname> <port>")
            # non-zero status indicates about encountered an issue with the command-line arguments
            raise ValueError("There are missing values at the command line, You must pay attention to the Usage")
            
        HOST = sys.argv[1]
        PORT = sys.argv[2]
        settlement = input("Please input a settlement here --> ")
        display_disribution(settlement, HOST, PORT)
    
    except ValueError as val_ex:
        print(val_ex)
    except WrongSettlementException as settl_ex:
        print(settl_ex)
    except requests.exceptions.RequestException as req_ex:
        print(req_ex)
    except Exception as ex:
        print(ex) 

if __name__ == "__main__":
    main()