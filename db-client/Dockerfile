FROM python:3-slim

WORKDIR /home/db-client

COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY . .

EXPOSE 5000
