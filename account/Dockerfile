FROM python:3-slim

WORKDIR /home/kafka-cockroach-client

COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY protobuf .
COPY consumer.py .

CMD ["python", "consumer.py"]
