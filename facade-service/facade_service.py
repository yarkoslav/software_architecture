from fastapi import FastAPI
from pydantic import BaseModel
import uuid
import requests
import random
from json import dumps
from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka.admin import NewPartitions


facade = FastAPI()
logging_services = ["http://logging-node1:8000/", "http://logging-node2:8000/", "http://logging-node3:8000/"]
messaging_services = ["http://messages-node1:8000/", "http://messages-node2:8000/"]
max_retries = 100


producer = KafkaProducer(bootstrap_servers=['kafka-server:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


# create partitions
admin_client = KafkaAdminClient(bootstrap_servers=['kafka-server:9092'])
partitions = {}
partitions["messages"] = NewPartitions(total_count=2)
admin_client.create_partitions(partitions)


@facade.get("/")
async def root():
    print(f"LOG from FACADE: GET")
    for _ in range(max_retries):
        try:
            logging_output = requests.get(random.choice(logging_services)).json()
            break
        except:
            logging_output = "logging error"
    messages_output = requests.get(random.choice(messaging_services)).json()
    return {"messages": "LOGGING: " + logging_output["messages"] + "\nMESSAGING: " + messages_output["messages"]}


class Message(BaseModel):
    text: str


@facade.post("/")
async def root(message: Message):
    print(f"LOG from FACADE: POST with body: {message}")
    message = message.text
    cur_uuid = str(uuid.uuid4())
    to_send = {"uuid": cur_uuid, "message": message}
    partition = random.randint(1, len(messaging_services))
    producer.send("messages", value=to_send, key=partition.to_bytes(2, 'big'))
    for _ in range(max_retries):
        try:
            requests.post(random.choice(logging_services),
                headers = {
                    'Content-type': 'application/json'
                },
                json = to_send,
            )
            break
        except:
            continue
