FROM python:3.9-slim

WORKDIR /opt/app/

COPY requirements.txt /opt/app
RUN pip install -r requirements.txt

COPY ./register_consul.py /opt/app
COPY ./facade_service.py /opt/app/

CMD ["/bin/bash", "-c", "python3 register_consul.py && uvicorn facade_service:facade --host 0.0.0.0 --port 8000"]