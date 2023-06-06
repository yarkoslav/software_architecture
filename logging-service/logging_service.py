from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import hazelcast
import consul
import os


consul_client = consul.Consul(host=os.environ["CONSUL_HOST"], port=os.environ["CONSUL_PORT"])


def get_kv_consul_value(key):
    _, data = consul_client.kv.get(key)
    value = data['Value'].decode('utf-8')
    return value

HALEZCAST_CLUSTER_NAME = get_kv_consul_value('hazelcast_cluster_name')
HAZELCAST_CLUSTER_MEMBERS = get_kv_consul_value('hazelcast_cluster_members').strip().split(",")
HAZELCAST_DB = get_kv_consul_value('hazelcast_db')


logging = FastAPI()

hazelcast_client = hazelcast.HazelcastClient(cluster_name=HALEZCAST_CLUSTER_NAME, 
                                             cluster_members=HAZELCAST_CLUSTER_MEMBERS)
db = hazelcast_client.get_map(HAZELCAST_DB).blocking()  

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
    db.put(message.uuid, message.message)
