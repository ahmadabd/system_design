from fastapi import FastAPI
import os

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Hello", "instance_port": os.getenv("PORT", "unknown")}

@app.get("/health")
def health():
    return {"status": "ok"}