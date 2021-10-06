import uuid

from kafka import KafkaProducer

from google.protobuf.any_pb2 import Any
from account.protobuf.messages_pb2 import Wrapper, Insert, State, Update, Transfer, Read


def wrap(request_id, outgoing):
    wrp = Wrapper()
    wrp.request_id = request_id
    message = Any()
    message.Pack(outgoing)
    wrp.message.CopyFrom(message)
    return wrp


def send_insert_message(identifier: str, balance: int = 10000):
    insert = Insert()
    insert.id = identifier
    insert.state.CopyFrom(State(balance=balance, fields={}))
    request_id = str(uuid.uuid4()).replace('-', '')
    serialized_wrapped = wrap(request_id, insert).SerializeToString()
    future = producer.send('insert', key=identifier.encode('utf8'), value=serialized_wrapped)
    future.get()


def send_transfer_message(in_id: str, out_id: str, amount: int = 10):
    transfer = Transfer()
    transfer.outgoing_id = out_id
    transfer.incoming_id = in_id
    transfer.amount = amount
    request_id = str(uuid.uuid4()).replace('-', '')
    serialized_wrapped = wrap(request_id, transfer).SerializeToString()
    future = producer.send('transfer', key=out_id.encode('utf8'), value=serialized_wrapped)
    future.get()


def send_update_message(up_id: str, balance: int):
    update = Update()
    update.id = up_id
    update.updates["balance"] = str(balance)
    request_id = str(uuid.uuid4()).replace('-', '')
    serialized_wrapped = wrap(request_id, update).SerializeToString()
    future = producer.send('read', key=up_id.encode('utf8'), value=serialized_wrapped)
    future.get()


def send_read_message(read_id: str):
    read = Read()
    read.id = read_id
    request_id = str(uuid.uuid4()).replace('-', '')
    serialized_wrapped = wrap(request_id, read).SerializeToString()
    future = producer.send('read', key=read_id.encode('utf8'), value=serialized_wrapped)
    future.get()


producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


n_records = 100
for i in range(n_records):
    send_insert_message(str(i))

send_transfer_message("1", "2", 10000000)
send_transfer_message("3", "4")
send_update_message("5", 99999)
send_read_message("5")
