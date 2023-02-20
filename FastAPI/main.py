from fastapi import FastAPI
from pydantic import BaseModel

app= FastAPI()

class myNameRequest(BaseModel):
    n:str

@app.get("/hello")
async def hello_world() -> dict:
    return {"message": "Hello world!"}

@app.post("/user_name")
async def your_name(myName: myNameRequest)-> dict:
    return {"name": myName.n}