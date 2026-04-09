from fastapi import FastAPI
import os

app = FastAPI()

@app.get("/")
def read_root():
    return {"msg": "API Salud RM funcionando"}

@app.get("/centros")
def get_centros():
    return {"data": "centros de salud (placeholder)"}