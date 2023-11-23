from fastapi import FastAPI
from pydantic import BaseModel
from datetime import time

class AlertsDemand(BaseModel):
    settlement: str 
    start_time: str
    end_time: str
