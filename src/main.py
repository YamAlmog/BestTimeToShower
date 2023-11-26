from AlertsAggregator import AlertsAggregator
from OrefAlertsRetrieve import OrefAlertsRetrieveData  
from fastapi import FastAPI, HTTPException 
import pandas as pd
import requests
from Errors import RetrieveDataException
from models import AlertsQueryInput
from datetime import datetime, timedelta
app = FastAPI()

CSV_FILE_PATH = "C:/Projects/Python/Projects/BestTimeToShower/data/data.csv"
dataframe = pd.read_csv(CSV_FILE_PATH)
getdata = OrefAlertsRetrieveData()
alert_aggregator = AlertsAggregator(dataframe)



@app.post("/orefalerts")
async def get_oref_alert(from_date: str, to_date: str):
    try:
        global alert_aggregator
        getdata.get_oref_alarms(from_date, to_date)
        df = pd.read_csv(CSV_FILE_PATH)
        alert_aggregator = AlertsAggregator(df)
        return {"message": "Up-date the alerts data csv"}
    except RetrieveDataException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as ex:
        raise HTTPException(status_code=401, detail=str(ex))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/best_time_to_shower")
async def find_best_time_to_shower(alerts_demand_obj: AlertsQueryInput):
    try:
        best_time = alert_aggregator.best_time_to_shower(alerts_demand_obj.settlement, alerts_demand_obj.start_time, alerts_demand_obj.end_time)
        return best_time
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))
    

@app.post("/worst_time_to_shower")
async def find_worst_time_to_shower(alerts_demand_obj: AlertsQueryInput):
    try:
        worst_time = alert_aggregator.worst_time_to_shower(alerts_demand_obj.settlement, alerts_demand_obj.start_time, alerts_demand_obj.end_time)
        return worst_time
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))
    

@app.get("/alerts_count")
async def get_alerts_count(settlement: str):
    try:
        alert_amount = alert_aggregator.alerts_count(settlement)
        return alert_amount
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except requests.exceptions.RequestException as req_err:
        raise HTTPException(status_code=402, detail=str(req_err))
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))
    
@app.get("/total_alerts_count")
async def get_total_alerts_count():
    return alert_aggregator.total_alerts_count()


@app.get("/poorest_city")
async def get_poorest_area():
    return alert_aggregator.poorest_area()


@app.get("/get_distribution")
async def get_distribution(settlement: str):
    try:
        return alert_aggregator.display_dist(settlement)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except requests.exceptions.RequestException as req_err:
        raise HTTPException(status_code=402, detail=str(req_err))
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))
    

@app.get("/get_all_settlement")
async def get_all_settlement():        
        response = alert_aggregator.retrieve_all_settlement()
        return response

