import consul
import os


consul_client = consul.Consul(host=os.environ["CONSUL_HOST"], port=os.environ["CONSUL_PORT"])

consul_client.agent.service.register(
    name="messages",
    service_id=os.environ["SERVICE_NAME"],
    address=os.environ["SERVICE_ADDRESS"],
    port=int(os.environ["SERVICE_PORT"])
)