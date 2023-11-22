from AlertsAggregator import AlertsAggregator, GetAlertsData 
from fastapi import FastAPI, HTTPException 
import pandas as pd
from Errors import AlertAggregatorException

app = FastAPI()

CSV_FILE_PATH = "C:/Projects/Python/Projects/BestTimeToShower/data/data.csv"
dataframe = pd.read_csv(CSV_FILE_PATH)
getdata = GetAlertsData()
alert_aggregator = AlertsAggregator(dataframe)



app.get("/orefalerts")
async def get_oref_alert(from_date: str, to_date: str):
    try:
        getdata.get_oref_alarms(from_date, to_date)
        return {"message": "Up-date the alerts data csv"}
    except AlertAggregatorException as e:
        raise HTTPException(status_code=404, detail=str(e))


app.get("/best_time_to_shower")
async def get_best_time_to_shower(settlement: str, start_time: str, end_time: str):
    try:
        best_time = alert_aggregator.best_time_to_shower(settlement, start_time, end_time)
        return best_time
    except ValueError as e:
        raise ValueError(status_code=400, detail=str(e))
    

app.get("/worst_time_to_shower")
async def get_worst_time_to_shower(settlement: str, start_time: str, end_time: str):
    try:
        worst_time = alert_aggregator.worst_time_to_shower(settlement, start_time, end_time)
        return worst_time
    except ValueError as e:
        raise ValueError(status_code=400, detail=str(e))
    

app.get(":/alerts_count")
async def get_alerts_count(settlement: str):
    try:
        alert_amount = alert_aggregator.alerts_count(settlement)
        return alert_amount
    except ValueError as e:
        raise ValueError(status_code=400, detail=str(e))
    
app.get("/total_alerts_count")
async def get_total_alerts_count():
    return alert_aggregator.total_alerts_count()


app.get("/poorest_city")
async def get_poorest_city():
    return alert_aggregator.poorest_city()