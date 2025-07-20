from fastapi import FastAPI
from fastapi.responses import JSONResponse

app=FastAPI()




@app.get("/health")
async def health():
    return {
        "status":"ok",
        "message":"Service is running",
    }