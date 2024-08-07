from alerts_aggregator import AlertsAggregator
from oref_alert_indexer import OrefAlertsIndexer  
from fastapi import FastAPI, HTTPException , Depends, Header
import pandas as pd
import requests
from errors import OrefAPIException, WrongSettlementException, NoAlarmsException, InvalidSettlement, SqlDatabaseException
from models import AlertsQueryInput, AlertType
from datetime import datetime, timedelta
from sql_database import OrefAlertsDB
import os

app = FastAPI()
OREF_TABLE = os.getenv("OREF_TABLE")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
CSV_FILE_PATH = os.getenv("ALERTS_DATA_FILE")
alert_indexer = OrefAlertsIndexer()
sql_instance = OrefAlertsDB(OREF_TABLE)


# Define the function to be called on app initialization
def init_app(table_name:str):
    print("App is starting up...")
    
    # read data from SQL server and load to dataframe 
    dataframe = sql_instance.retrieve_data_from_oref_table(table_name)
    # init alerts aggeragot 
    global alert_aggregator
    alert_aggregator = AlertsAggregator(dataframe)

# Register the function to be called on startup
@app.on_event("startup")
async def startup_event():
    init_app(OREF_TABLE)


def get_api_key(api_key: str = Header(..., convert_underscores=False)):
    if api_key == ADMIN_API_KEY:
        return api_key
    else:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
@app.post("/sync_oref_alerts")
async def get_oref_alert(from_date: str, to_date: str, api_key: str = Depends(get_api_key)):
    try:
        df = alert_indexer.arrange_alarms_within_sql_database(from_date, to_date, OREF_TABLE)
        alert_aggregator.reload_data(df)
        return {"message": f"You updated the alerts sql database"}
    except OrefAPIException as ex:
        raise HTTPException(status_code=404, detail=str(ex))
    except SqlDatabaseException as ex:
        raise HTTPException(status_code=401, detail=str(ex))
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Unknown Error: {ex}")


@app.post("/best_time_to_shower")
async def find_best_time_to_shower(alerts_query: AlertsQueryInput):
    try:
        best_time = alert_aggregator.best_time_to_shower(alerts_query.settlement, alerts_query.start_time, alerts_query.end_time)
        return {"message" : f"The best time to take a shower is at: {best_time}"}
    except NoAlarmsException as ex:
        return {"message" : str(ex)}
    except InvalidSettlement as ex:
        raise HTTPException(status_code=400, detail=str(ex))
    except WrongSettlementException as ex:
        raise HTTPException(status_code=401, detail=str(ex)) 
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Unknown Error: {ex}")
    

@app.post("/worst_time_to_shower")
async def find_worst_time_to_shower(alerts_query: AlertsQueryInput):
    try:
        worst_time = alert_aggregator.worst_time_to_shower(alerts_query.settlement, alerts_query.start_time, alerts_query.end_time)
        return {"message" : f"The worst time to take a shower is at: {worst_time}"}
    except NoAlarmsException as ex:
        return {"message" : str(ex)}
    except InvalidSettlement as ex:
        raise HTTPException(status_code=400, detail=str(ex))
    except WrongSettlementException as ex:
        raise HTTPException(status_code=401, detail=str(ex)) 
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Unknown Error: {ex}")
    

@app.get("/alerts_count")
async def get_alerts_count(settlement: str):
    try:
        alert_amount = alert_aggregator.alerts_count(settlement)
        return {"message" : f"The total amount of alerts in {settlement} is: {alert_amount}"} 
    except NoAlarmsException as ex:
        return {"message" : str(ex)}
    except InvalidSettlement as ex:
        raise HTTPException(status_code=400, detail=str(ex))
    except requests.exceptions.RequestException as ex:
        raise HTTPException(status_code=402, detail=str(ex))
    except WrongSettlementException as ex:
        raise HTTPException(status_code=401, detail=str(ex)) 
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Unknown Error: {ex}")
    
@app.get("/total_alerts_count")
async def get_total_alerts_count():
    total_alerts = alert_aggregator.total_alerts_count()
    return {"message" : f"The total amount of alerts is: {total_alerts}"}


@app.get("/most_hitted_settlement")
async def get_most_hitted_settlement(alert_type:AlertType):
    poorest_city = alert_aggregator.most_hitted_settlement(alert_type)
    return {"message": f"The area that suffers the most from {alert_type} alarms is: {poorest_city}"}


@app.get("/get_hourly_distribution")
async def get_hourly_distribution(settlement: str):
    try:
        return alert_aggregator.get_alerts_distribution(settlement)
    except NoAlarmsException as ex:
        return {"message" : str(ex)}
    except InvalidSettlement as ex:
        raise HTTPException(status_code=400, detail=str(ex))
    except requests.exceptions.RequestException as ex:
        raise HTTPException(status_code=402, detail=str(ex))
    except WrongSettlementException as ex:
        raise HTTPException(status_code=401, detail=str(ex)) 
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Unknown Error: {ex}")
    
@app.get("/get_distribution_per_day")
async def get_distribution_per_day(settlement: str):
    try:
        return alert_aggregator.get_alerts_distribution_per_day(settlement)
    except NoAlarmsException as ex:
        return {"message" : str(ex)}
    except InvalidSettlement as ex:
        raise HTTPException(status_code=400, detail=str(ex))
    except requests.exceptions.RequestException as ex:
        raise HTTPException(status_code=402, detail=str(ex))
    except WrongSettlementException as ex:
        raise HTTPException(status_code=401, detail=str(ex)) 
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Unknown Error: {ex}")


@app.get("/get_all_settlement")
async def get_all_settlement():        
        settlement_list = alert_aggregator.retrieve_all_settlement()
        return {"message": f"The settlements in the df are: {settlement_list}"}


@app.get("/alerts_types")
async def get_alerts_types():
    all_types = alert_aggregator.get_all_alerts_types()
    return {"message" : f"The total amount of alerts is: {all_types}"}

