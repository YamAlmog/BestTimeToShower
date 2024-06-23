from enum import Enum
from pydantic import BaseModel
from datetime import time, date

class AlertsQueryInput(BaseModel):
    settlement: str 
    start_time: time
    end_time: time

class AlertCountPerHour(BaseModel):
    hour : str
    count : int

class AlertCountPerDay(BaseModel):
    date: date
    count: int

class AlertType(str, Enum):
    missiles = "Missiles"
    hostile_aircraft_intrusion = "Hostile aircraft intrusion"