from fastapi import FastAPI
from json import loads
from kafka import KafkaConsumer
import threading
import consul
import os


consul_client = consul.Consul(host=os.environ["CONSUL_HOST"], port=os.environ["CONSUL_PORT"])


def get_kv_consul_value(key):
    _, data = consul_client.kv.get(key)
    value = data['Value'].decode('utf-8')
    return value


KAFKA_BOOTSTRAP_SERVERS = get_kv_consul_value('kafka_bootstrap_servers').strip().split()
KAFKA_TOPIC = get_kv_consul_value('kafka_topic')
KAFKA_GROUP = get_kv_consul_value('kafka_group')

db = []


consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=KAFKA_GROUP,
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
