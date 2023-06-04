from fastapi import FastAPI
from json import loads
from kafka import KafkaConsumer
import threading


db = []


consumer = KafkaConsumer(
        'messages',
        bootstrap_servers=['kafka-server:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))


def read_mq():
    for message in consumer:
        message = message.value["message"]
        print(f"LOG from MESSAGES: FROM MQ: {message}")
        db.append(message)


messages = FastAPI()


@messages.get("/")
async def root():
    print(f"LOG from MESSAGES: GET")
    return {"messages": " ".join(db)}


@messages.on_event("startup")
def startup_event():
    thread = threading.Thread(target=read_mq)
    thread.start()
