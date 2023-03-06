from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging = FastAPI()

db = {}

@logging.get("/")
async def root():
    print(f"LOG from LOGGING: GET")
    return {"messages": " ".join(list(db.values()))}


class Message(BaseModel):
    uuid: str
    message: str

@logging.post("/")
async def root(message: Message):
    print(f"LOG from LOGGING: POST with body: {message}")
    if message.uuid in db:
        raise HTTPException(status_code=406, detail="UUID already present")
    else:
        db[message.uuid] = message.message
