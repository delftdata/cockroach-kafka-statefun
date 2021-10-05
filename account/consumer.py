import os
import psycopg2
from aiopg.sa import create_engine
from psycopg2.errors import SerializationFailure, UniqueViolation
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncio
import uvloop
from google.protobuf.any_pb2 import Any
from google.protobuf.message import DecodeError

from protobuf.messages_pb2 import Wrapper, Insert, Read, Update, Transfer, Response


async def send_response(request_id: str, status_code: int, message=None):
    producer = AIOKafkaProducer(bootstrap_servers=[os.environ.get("KAFKA_URL")])
    response = Response(request_id=request_id, status_code=status_code)
    if message:
        out = Any()
        out.Pack(message)
        response.message.CopyFrom(out)
    await producer.start()
    try:
        # Produce message
        await producer.send_and_wait('responses', key=request_id.encode('utf8'), value=response.SerializeToString())
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()


class UnknownMessageException(Exception):
    pass


async def handle_insert(key: str, balance: int, engine):
    async with engine.acquire() as con:
        try:
            await con.execute("INSERT INTO accounts (id, balance) VALUES (%s, %s)", (key, balance))
        except UniqueViolation:
            return 400
    return 200


async def handle_read(key: str, engine):
    async with engine.acquire() as con:
        account = await con.execute("SELECT * FROM accounts WHERE id = %s", (key, ))
    return {'account': account}


async def handle_transfer(out_key: str, in_key: str, amount: int, engine):
    async with engine.acquire() as con:
        trans = await con.begin()
        try:
            # TODO check here
            await con.execute(
                "UPDATE accounts SET balance = balance - %s WHERE id = %s", (amount, out_key)
            )
            await con.execute(
                "UPDATE accounts SET balance = balance + %s WHERE id = %s", (amount, in_key)
            )
        except SerializationFailure:
            await trans.rollback()
            return 422
        # except Exception:
        #     pass
        #     # TODO or here
        else:
            await trans.commit()
    return 200


async def handle_update(key: str, balance: int, engine):
    async with engine.acquire() as con:
        await con.execute("UPSERT INTO accounts (id, balance) VALUES (%s, %s)", (key, balance))


async def consume(engine):
    consumer = AIOKafkaConsumer(
        'insert', 'update', 'read', 'transfer',
        bootstrap_servers=[os.environ.get("KAFKA_URL")],
        group_id="accounts_consumer_group",
        enable_auto_commit=False)
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            # print("consumed: ", msg.topic, msg.partition, msg.offset,
            #       msg.key, msg.value, msg.timestamp)
            try:
                wrapped = Wrapper().FromString(msg.value)
                request_id = wrapped.request_id
                message = wrapped.message
                if message.Is(Insert.DESCRIPTOR):
                    insert = Insert()
                    message.Unpack(insert)
                    status = await handle_insert(insert.id, insert.state.balance, engine)
                    await send_response(request_id, status)
                elif message.Is(Read.DESCRIPTOR):
                    read = Read()
                    message.Unpack(read)
                    await handle_read(read.id, engine)
                    await send_response(request_id, 200)
                    # account = await handle_read(read.id, engine)
                    # await send_response(request_id, 200, account)
                elif message.Is(Update.DESCRIPTOR):
                    update = Update()
                    message.Unpack(update)
                    await handle_update(update.id, int(update.updates["balance"]), engine)
                    await send_response(request_id, 200)
                elif message.Is(Transfer.DESCRIPTOR):
                    transfer = Transfer()
                    message.Unpack(transfer)
                    status = await handle_transfer(transfer.outgoing_id, transfer.incoming_id, transfer.amount, engine)
                    await send_response(request_id, status)
                else:
                    raise UnknownMessageException()
            except DecodeError:
                raise UnknownMessageException()
            finally:
                await consumer.commit()
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


def create_table():
    conn = psycopg2.connect(
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        sslmode=os.environ.get("DB_SSL"),
        host=os.environ.get("DB_HOST"),
        port=os.environ.get("DB_PORT"),
    )
    with conn.cursor() as cur:
        cur.execute(
              "CREATE DATABASE IF NOT EXISTS accounts;"
              "CREATE TABLE IF NOT EXISTS accounts (id STRING PRIMARY KEY, balance INT NOT NULL DEFAULT 0 CHECK (balance >= 0));"
        )
    conn.commit()
    print('Database initialized')
    conn.close()


async def run():
    async with create_engine(database=os.environ.get("DB_NAME"),
                             user=os.environ.get("DB_USER"),
                             password=os.environ.get("DB_PASSWORD"),
                             sslmode=os.environ.get("DB_SSL"),
                             host=os.environ.get("DB_HOST"),
                             port=os.environ.get("DB_PORT")) as engine:
        await consume(engine)


create_table()

uvloop.install()
asyncio.run(run())
