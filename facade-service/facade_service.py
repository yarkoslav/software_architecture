from fastapi import FastAPI
from pydantic import BaseModel
import uuid
import requests
import random
from json import dumps
from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka.admin import NewPartitions
import consul
import os


consul_client = consul.Consul(host=os.environ["CONSUL_HOST"], port=os.environ["CONSUL_PORT"])


def get_kv_consul_value(key):
    _, data = consul_client.kv.get(key)
    value = data['Value'].decode('utf-8')
    return value


KAFKA_BOOTSTRAP_SERVERS = get_kv_consul_value('kafka_bootstrap_servers').strip().split()
KAFKA_TOPIC = get_kv_consul_value('kafka_topic')


facade = FastAPI()
max_retries = 100


def get_url(service_name):
    _, service_data = consul_client.catalog.service(service_name)
    service_instance = random.choice(service_data)
    service_address = service_instance["ServiceAddress"]
    service_port = service_instance["ServicePort"]
    return f"http://{service_address}:{service_port}/"


def get_num(service_name):
    _, service_data = consul_client.catalog.service(service_name)
    return len(service_data)


producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


# create partitions
admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
partitions = {}
partitions[KAFKA_TOPIC] = NewPartitions(total_count=get_num("messages"))
admin_client.create_partitions(partitions)


@facade.get("/")
async def root():
    print(f"LOG from FACADE: GET")
    for _ in range(max_retries):
        try:
            logging_output = requests.get(get_url("logging")).json()
            break
        except:
            logging_output = "logging error"
    messages_output = requests.get(get_url("messages")).json()
    return {"messages": "LOGGING: " + logging_output["messages"] + "\nMESSAGING: " + messages_output["messages"]}


class Message(BaseModel):
    text: str


@facade.post("/")
async def root(message: Message):
    print(f"LOG from FACADE: POST with body: {message}")
    message = message.text
    cur_uuid = str(uuid.uuid4())
    to_send = {"uuid": cur_uuid, "message": message}
    partition = random.randint(1, get_num("messages"))
    producer.send(KAFKA_TOPIC, value=to_send, key=partition.to_bytes(2, 'big'))
    for _ in range(max_retries):
        try:
            requests.post(get_url("logging"),
                headers = {
                    'Content-type': 'application/json'
                },
                json = to_send,
            )
            break
        except:
            continue
