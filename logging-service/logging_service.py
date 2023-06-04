from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import hazelcast

logging = FastAPI()

hazelcast_client = hazelcast.HazelcastClient(cluster_name="micro-hazelcast", cluster_members=[
    "hazelcast-node1", 
    "hazelcast-node2",
    "hazelcast-node3"
])
db = hazelcast_client.get_map("db").blocking()  

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
