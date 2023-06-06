FROM python:3.9-slim

WORKDIR /opt/app/

COPY requirements.txt /opt/app/
RUN pip install -r /opt/app/requirements.txt

COPY ./register_consul.py /opt/app
COPY ./messages_service.py /opt/app/

CMD ["/bin/bash", "-c", "python3 register_consul.py && uvicorn messages_service:messages --host 0.0.0.0 --port 8000"]