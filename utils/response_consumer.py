from aiokafka import AIOKafkaConsumer
import asyncio
import uvloop


async def consume():
    consumer = AIOKafkaConsumer(
        'responses',
        bootstrap_servers='localhost:9092')
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                  msg.key, msg.value, msg.timestamp)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


uvloop.install()
asyncio.run(consume())
