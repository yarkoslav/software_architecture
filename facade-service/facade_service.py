from fastapi import FastAPI
from pydantic import BaseModel
import uuid
import requests
import random


facade = FastAPI()
logging_services = ["http://logging-node1:8000/", "http://logging-node2:8000/", "http://logging-node3:8000/"]
max_retries = 100


@facade.get("/")
async def root():
    print(f"LOG from FACADE: GET")
    for _ in range(max_retries):
        try:
            logging_output = requests.get(random.choice(logging_services)).json()
            break
        except:
            logging_output = "logging error"
    messages_output = requests.get("http://messages:8000/").json()
    return {"messages": logging_output["messages"] + " " + messages_output["messages"]}


class Message(BaseModel):
    text: str

@facade.post("/")
async def root(message: Message):
    print(f"LOG from FACADE: POST with body: {message}")
    message = message.text
    cur_uuid = str(uuid.uuid4())
    for _ in range(max_retries):
        try:
            requests.post(random.choice(logging_services),
                headers = {
                    'Content-type': 'application/json'
                },
                json = {"uuid": cur_uuid, "message": message},
            )
            break
        except:
            continue
