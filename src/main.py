from alerts_aggregator import AlertsAggregator
from oref_alert_indexer import OrefAlertsIndexer  
from fastapi import FastAPI, HTTPException , Depends, Header
import pandas as pd
import requests
from errors import OrefAPIException, WrongSettlementException, NoAlarmsException, InvalidSettlement
from models import AlertsQueryInput
from datetime import datetime, timedelta
import os

app = FastAPI()

ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
CSV_FILE_PATH = os.getenv("ALERTS_DATA_FILE")
dataframe = pd.read_csv(CSV_FILE_PATH)
alert_indexer = OrefAlertsIndexer()
alert_aggregator = AlertsAggregator(dataframe)

def get_api_key(api_key: str = Header(..., convert_underscores=False)):
    if api_key == ADMIN_API_KEY:
        return api_key
    else:
        raise HTTPException(status_code=401, detail="Invalid API key")


@app.post("/sync_oref_alerts")
async def get_oref_alert(from_date: str, to_date: str, api_key: str = Depends(get_api_key)):
    try:
        df = alert_indexer.arrange_alarms_within_csv(from_date, to_date)
        alert_aggregator.reload_data(df)
        return {"message": f"You updated the alerts database"}
    except OrefAPIException as ex:
        raise HTTPException(status_code=404, detail=str(ex))
    except PermissionError as ex:
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


@app.get("/poorest_city")
async def get_poorest_area():
    poorest_city = alert_aggregator.poorest_area()
    return {"message": f"The area that suffers the most from alarms is: {poorest_city}"}

@app.get("/get_distribution")
async def get_distribution(settlement: str):
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
    

@app.get("/get_all_settlement")
async def get_all_settlement():        
        settlement_list = alert_aggregator.retrieve_all_settlement()
        return {"message": f"The settlements in the df are: {settlement_list}"}

