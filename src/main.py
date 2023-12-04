from alerts_aggregator import AlertsAggregator
from oref_alert_class import OrefAlertsIndexer  
from fastapi import FastAPI, HTTPException 
import pandas as pd
import requests
from errors import RetrieveDataException, WrongSettlementException, NoAlarmsException, InvalidSettlement
from models import AlertsQueryInput
from datetime import datetime, timedelta
app = FastAPI()

CSV_FILE_PATH = "C:/Projects/Python/Projects/BestTimeToShower/data/data.csv"
dataframe = pd.read_csv(CSV_FILE_PATH)
getdata = OrefAlertsIndexer()
alert_aggregator = AlertsAggregator(dataframe)



@app.post("/sync_oref_alerts")
async def get_oref_alert(from_date: str, to_date: str):
    try:
        global alert_aggregator
        getdata.get_oref_alarms(from_date, to_date)
        df = pd.read_csv(CSV_FILE_PATH)
        alert_aggregator = AlertsAggregator(df)
        return {"message": "Up-date the alerts data csv"}
    except RetrieveDataException as ex:
        raise HTTPException(status_code=404, detail=str(ex))
    except PermissionError as ex:
        raise HTTPException(status_code=401, detail=str(ex))
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Unknown Error: {ex}")


@app.post("/best_time_to_shower")
async def find_best_time_to_shower(alerts_query: AlertsQueryInput):
    try:
        best_time = alert_aggregator.best_time_to_shower(alerts_query.settlement, alerts_query.start_time, alerts_query.end_time)
        return {"message" : best_time}
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
        return {"message" : worst_time}
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
        response = alert_aggregator.alerts_count(settlement)
        return {"message" : response} 
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
    response = alert_aggregator.total_alerts_count()
    return {"message" : response}


@app.get("/poorest_city")
async def get_poorest_area():
    response = alert_aggregator.poorest_area()
    return {"message": response}

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
        response = alert_aggregator.retrieve_all_settlement()
        return {"message": response}

