FROM python:3.9-slim

WORKDIR /opt/app/

COPY requirements.txt /opt/app
RUN pip install -r requirements.txt

COPY ./facade_service.py /opt/app/

ENTRYPOINT ["uvicorn", "facade_service:facade", "--host", "0.0.0.0", "--port", "8000"]