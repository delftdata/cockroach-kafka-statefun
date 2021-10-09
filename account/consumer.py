import os
import psycopg2
from aiopg.sa import create_engine
from psycopg2.errors import SerializationFailure, UniqueViolation, CheckViolation, ObjectNotInPrerequisiteState
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.errors import UnknownTopicOrPartitionError, KafkaConnectionError
import asyncio
import uvloop
from google.protobuf.any_pb2 import Any
from google.protobuf.message import DecodeError
import datetime
import logging
import time

from protobuf.messages_pb2 import Wrapper, Insert, Read, Update, Transfer, Response, State


logging.Formatter.formatTime = (lambda self, record, datefmt: datetime.datetime.
                                fromtimestamp(record.created, datetime.timezone.utc).astimezone().isoformat())

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
                    level=logging.INFO)


async def send_response(producer, request_id: str, status_code: int, message=None):
    response = Response(request_id=request_id, status_code=status_code)
    if message:
        fields = {'field0': message.field0, 'field1': message.field1, 'field2': message.field2,
                  'field3': message.field3, 'field4': message.field4, 'field5': message.field5,
                  'field6': message.field6, 'field7': message.field7, 'field8': message.field8,
                  'field9': message.field9}
        state_msg = State(balance=message.balance, fields=fields)
        out = Any()
        out.Pack(state_msg)
        response.message.CopyFrom(out)
    # Produce message
    await producer.send_and_wait('responses', key=request_id.encode('utf8'), value=response.SerializeToString())


class UnknownMessageException(Exception):
    pass


async def handle_insert(key: str, balance: int, fields: dict, engine):
    update_fields = " ,".join(list(fields.keys()))
    input_str_query = ", %s" * len(fields.keys())
    async with engine.acquire() as con:
        try:
            await con.execute(f"INSERT INTO accounts (id, balance, {update_fields}) VALUES (%s, %s{input_str_query})",
                              (key, balance, *list(fields.values())))
        except UniqueViolation:
            return 400
    return 200


async def handle_read(key: str, engine):
    async with engine.acquire() as con:
        account = await (await con.execute("SELECT * FROM accounts WHERE id = %s", (key, ))).first()
    return account


async def handle_transfer(out_key: str, in_key: str, amount: int, engine):
    async with engine.acquire() as con:
        trans = await con.begin()
        try:
            await con.execute(
                "UPDATE accounts SET balance = balance - %s WHERE id = %s", (amount, out_key)
            )
            await con.execute(
                "UPDATE accounts SET balance = balance + %s WHERE id = %s", (amount, in_key)
            )
        except SerializationFailure:
            # Error in the transaction
            await trans.rollback()
            return 422
        except CheckViolation:
            # Not enough credit
            await trans.rollback()
            return 401
        else:
            # Successful transaction
            await trans.commit()
            return 200


async def handle_update(key: str, fields: dict, engine):
    update_fields = " ,".join(list(fields.keys()))
    input_str_query = ", %s" * len(fields.keys())
    async with engine.acquire() as con:
        await con.execute(f"UPSERT INTO accounts (id, {update_fields}) VALUES (%s{input_str_query})",
                          (key, *list(fields.values())))


async def consume(engine):
    logging.info("Ready to consume messages")
    consumer = AIOKafkaConsumer(
        'insert', 'update', 'read', 'transfer',
        bootstrap_servers=[os.environ.get("KAFKA_URL")],
        group_id="accounts_consumer_group",
        enable_auto_commit=False)
    producer = AIOKafkaProducer(bootstrap_servers=[os.environ.get("KAFKA_URL")])
    while True:
        try:
            await producer.start()
            await consumer.start()
        except (UnknownTopicOrPartitionError, KafkaConnectionError):
            time.sleep(1)
            logging.info("Waiting for topics to be created")
            continue
        break
    try:
        # Consume messages
        async for msg in consumer:
            logging.debug(f"Consumed: {msg.topic} {msg.partition} {msg.offset} {msg.key} {msg.value} {msg.timestamp}")
            try:
                wrapped = Wrapper().FromString(msg.value)
                request_id = wrapped.request_id
                message = wrapped.message
                if message.Is(Insert.DESCRIPTOR):
                    insert = Insert()
                    message.Unpack(insert)
                    status = await handle_insert(insert.id, insert.state.balance, insert.state.fields, engine)
                    await send_response(producer, request_id, status)
                elif message.Is(Read.DESCRIPTOR):
                    read = Read()
                    message.Unpack(read)
                    message = await handle_read(read.id, engine)
                    await send_response(producer, request_id, 200, message=message)
                elif message.Is(Update.DESCRIPTOR):
                    update = Update()
                    message.Unpack(update)
                    await handle_update(update.id, update.updates, engine)
                    await send_response(producer, request_id, 200)
                elif message.Is(Transfer.DESCRIPTOR):
                    transfer = Transfer()
                    message.Unpack(transfer)
                    status = await handle_transfer(transfer.outgoing_id, transfer.incoming_id, transfer.amount, engine)
                    await send_response(producer, request_id, status)
                else:
                    raise UnknownMessageException()
            except DecodeError:
                raise UnknownMessageException()
            finally:
                await consumer.commit()
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await producer.stop()
        await consumer.stop()


def cleanup_db():
    conn = psycopg2.connect(
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        sslmode=os.environ.get("DB_SSL"),
        host=os.environ.get("DB_HOST"),
        port=os.environ.get("DB_PORT"),
    )
    with conn.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS accounts CASCADE;")
    conn.commit()
    conn.close()


def create_table():
    conn = psycopg2.connect(
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        sslmode=os.environ.get("DB_SSL"),
        host=os.environ.get("DB_HOST"),
        port=os.environ.get("DB_PORT"),
    )
    while True:
        try:
            with conn.cursor() as cur:
                cur.execute(
                      "CREATE DATABASE IF NOT EXISTS accounts;"
                      "CREATE TABLE IF NOT EXISTS accounts (id STRING PRIMARY KEY, "
                      "                                     balance INT NOT NULL DEFAULT 0 CHECK (balance >= 0), "
                      "                                     field0 STRING, field1 STRING, field2 STRING, field3 STRING, "
                      "                                     field4 STRING, field5 STRING, field6 STRING, field7 STRING,"
                      "                                     field8 STRING, field9 STRING);"
                )
            conn.commit()
        except ObjectNotInPrerequisiteState:
            continue
        else:
            break
    conn.close()


async def run():
    async with create_engine(database=os.environ.get("DB_NAME"),
                             user=os.environ.get("DB_USER"),
                             password=os.environ.get("DB_PASSWORD"),
                             sslmode=os.environ.get("DB_SSL"),
                             host=os.environ.get("DB_HOST"),
                             port=os.environ.get("DB_PORT")) as engine:
        await consume(engine)


cleanup_db()

create_table()

uvloop.install()
asyncio.run(run())
