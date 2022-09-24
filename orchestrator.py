import asyncio
import os
import re
from dataclasses import dataclass, field
from typing import Dict, List

from aio_pika import DeliveryMode, Message, connect
from aio_pika.abc import AbstractIncomingMessage
from dotenv import load_dotenv
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

Base = declarative_base()
engine = create_async_engine(
    f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@0.0.0.0/twitch",
    echo=False,
)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def create_db():
    # async with engine.begin() as conn:
    #     await conn.run_sync(Base.metadata.drop_all)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


class PrivateMessage(Base):
    __tablename__ = "chat"

    id = Column(Integer, primary_key=True)
    message = Column(String)
    timestamp = Column(DateTime)
    channel = Column(String)
    nick = Column(String)

    # required in order to access columns with server defaults
    # or SQL expression defaults, subsequent to a flush, without
    # triggering an expired load
    __mapper_args__ = {"eager_defaults": True}


async def push_to_db(data):
    # print(data)
    # tweet = PrivateMessage(
    #     timestamp=data["timestamp"],
    #     channel=data["channel"],
    #     nick=data["username"],
    #     message=data["message"],
    # )
    list_private_messages = [PrivateMessage(**d) for d in data]
    print("pushing to db")
    async with async_session() as session:
        async with session.begin():
            session.add_all(list_private_messages)
            await session.commit()


@dataclass
class SubscriberInfo:
    msg_per_sec: int = 0
    channels: List = field(default_factory=list)


@dataclass
class Orchestrator:
    subscribers: Dict[str, Dict] = field(default_factory=dict)
    channels_monitored: List = field(default_factory=list)
    channels_new: List = field(default_factory=list)
    last_message: Dict = field(default_factory=dict)
    buffer_data: List = field(default_factory=list)

    async def on_message(self, message: AbstractIncomingMessage) -> None:
        async with message.process():
            # print(f"     Message body is: {message.body!r}")
            # print(f"     Message body is: {message.headers['x-id']}")
            # print(f"     Message body is: {message.timestamp}")
            messages = message.body.decode("utf-8").split("\r\n")
            for msg in messages:
                print(msg)
                if msg.startswith(b"SUBSCRIBE"):
                    print("got new subscriber")
                    id_subscribers = str(msg.split(b" ")[1], "utf-8")
                    self.subscribers[id_subscribers] = SubscriberInfo()
                msg_utf8 = msg.decode()
                is_privmsg = re.match(
                    r"^:[a-zA-Z0-9_]+\![a-zA-Z0-9_]+@[a-zA-Z0-9_]+"
                    r"\.tmi\.twitch\.tv "
                    r"PRIVMSG #[a-zA-Z0-9_]+ :.+$",
                    msg_utf8,
                )
                if is_privmsg:
                    data = {
                        "channel": re.findall(
                            r"^:.+![a-zA-Z0-9_]+"
                            r"@[a-zA-Z0-9_]+"
                            r".+ "
                            r"PRIVMSG (.*?) :",
                            msg_utf8,
                        )[0],
                        "timestamp": message.timestamp,
                        "nick": re.findall(r"^:([a-zA-Z0-9_]+)!", msg_utf8)[0],
                        "message": re.findall(
                            r"PRIVMSG #[a-zA-Z0-9_]+ :(.+)$", msg_utf8
                        )[0],
                    }
                    self.buffer_data.append(data)
                    if len(self.buffer_data) > 1500:
                        await push_to_db(self.buffer_data)
                        self.buffer_data = []

    async def consumer(self) -> None:
        connection = await connect(
            f"amqp://{RABBITMQ_USERNAME}:{RABBITMQ_PASSWORD}@localhost"
        )

        async with connection:
            # Creating a channel
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)

            # Declaring queue
            queue = await channel.declare_queue(
                "task_queue",
                durable=False,
                # durable=True,
            )

            # Start listening the queue with name 'task_queue'
            await queue.consume(self.on_message)

            print(" [*] Waiting for messages. To exit press CTRL+C")
            await asyncio.Future()

    async def producer(self) -> None:
        # Perform connection
        connection = await connect(
            f"amqp://{RABBITMQ_USERNAME}:{RABBITMQ_PASSWORD}@localhost"
        )
        async with connection:
            # Creating a channel
            channel = await connection.channel()
            message = Message(
                b"Hello World!",
                delivery_mode=DeliveryMode.PERSISTENT,
            )

            # Sending the message
            for sub in self.subscribers:
                await channel.default_exchange.publish(
                    message,
                    routing_key=sub,
                )

            print(f" [x] Sent {message!r}")
            print(f" [x] Sent {message.body!r}")

    async def infinite_producer(self) -> None:
        while True:
            if self.subscribers:
                print("producing")
                try:
                    await self.producer()
                except Exception as exp:
                    raise exp

            await asyncio.sleep(10)

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(create_db())
        # await push_to_db({"channel": "test", "username": "test", "message": "test"})
        # loop.create_task(
        #     push_to_db({"channel": "test", "username": "test", "message": "test"})
        # )
        # loop.create_task(self.consumer())
        # loop.create_task(self._push_to_db())
        # loop.create_task(self.infinite_producer())
        cors = asyncio.gather(self.consumer())
        loop.run_until_complete(cors)
        # loop.run_forever()
        print("done")


if __name__ == "__main__":
    orchestrator = Orchestrator()
    orchestrator.run()
