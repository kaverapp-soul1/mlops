from fastapi import FastAPI
from fastapi.responses import JSONResponse

app=FastAPI()


@app.get("/")
async def root():
    return JSONResponse(content={"message": "Hello, World!"})

@app.get("/health")
async def health():
    return {
        "status":"ok",
        "message":"Service is running",
    }