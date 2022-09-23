import argparse
import asyncio
import datetime
import os
import random
import string
from dataclasses import dataclass, field
from typing import List

from aio_pika import DeliveryMode, Message, connect
from aio_pika.abc import AbstractIncomingMessage
from dotenv import load_dotenv

load_dotenv()
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")


@dataclass
class Subscriber:
    _id: str = "".join([random.choice(string.ascii_letters) for _ in range(5)])
    server: str = "irc.chat.twitch.tv"
    token: str = "oauth"
    nickname: str = "justinfan" + "".join(
        [random.choice(string.digits) for _ in range(10)]
    )
    port: int = 6667
    channels: List = field(default_factory=lambda: ["#samueletienne"])
    is_connected: bool = False
    n_messages: int = 0
    n_channels: int = 0
    n_messages_last: int = 0
    last_message: bytes = b""

    async def send(self, message: str) -> None:
        self.writer.write(f"{message}\r\n".encode())
        await self.writer.drain()

    async def connect(self) -> None:
        exp = 0
        connected = False
        while not connected:
            try:
                self.reader, self.writer = await asyncio.open_connection(
                    self.server, self.port
                )
                connected = True
                print("Connected to Twitch IRC")

            # retry connection at increasing intervals
            except ConnectionResetError as e:
                print(e)
                print(f"Connection to Twitch failed. Retrying in {2**exp} second(s)...")
                await asyncio.sleep(2**exp)
                exp += 1
        await self.send(f"PASS {self.token}")
        await self.send(f"NICK {self.nickname}")
        received = await self.reader.readuntil(b"\r\n")
        if not received.startswith(b":tmi.twitch.tv 001"):
            raise IOError("Twitch did not accept the username-oauth combination")

        for i, channel in enumerate(self.channels):
            await self.send(f"JOIN {channel}")
            self.n_channels += 1
            if self.n_channels > 5000:
                await asyncio.sleep(0.5)
            else:
                await asyncio.sleep(0.2)
            self.is_connected = True

        # await self.send("CAP REQ :twitch.tv/tags")
        # await self.send("CAP REQ :twitch.tv/membership")

    async def notifications(self):
        while True:
            await asyncio.sleep(10)
            # print(f"Messages: {self.n_messages}")
            print(f"timestamp: {datetime.datetime.now()}")
            print(f"Channels: {self.n_channels}")
            diff = self.n_messages - self.n_messages_last
            print(f"Diff: {diff}")
            # print(f"last message: {self.last_message}")
            self.n_messages_last = self.n_messages
            if not diff:
                print("No messages received in the last 5 seconds")
                print("Reconnecting...")
                # raise ValueError("stop")

    async def pong(self) -> None:
        if self.is_connected:
            await self.send("PONG :tmi.twitch.tv")

    async def read_chat(self) -> None:
        while not self.is_connected:
            await asyncio.sleep(0.1)

        while True:
            data = await self.reader.readuntil(b"\r\n")
            self.n_messages += 1
            print(data)
            if data.startswith(b"PING"):
                parsed = data.decode().split(" ")[-1]
                await self.send(f"PONG {parsed}")
            else:
                await self.produce(data)

    async def infinite_read_chat(self):
        n_failure = 0
        while True:
            try:
                await self.read_chat()
            except Exception as exp:
                n_failure += 1
                if n_failure > 5:
                    raise exp
                await asyncio.sleep(5)
                continue

    async def produce(self, message_body) -> None:
        connection = await connect(
            f"amqp://{RABBITMQ_USERNAME}:{RABBITMQ_PASSWORD}@localhost"
        )
        async with connection:
            # Creating a channel
            channel = await connection.channel()

            message = Message(
                message_body,
                timestamp=datetime.datetime.now(),
                headers={"x-id": self._id},
                delivery_mode=DeliveryMode.PERSISTENT,
            )

            # Sending the message
            await channel.default_exchange.publish(
                message,
                routing_key="task_queue",
            )

            print(f" [x] Sent {message_body!r}")

    async def consume(self) -> None:
        connection = await connect(
            f"amqp://{RABBITMQ_USERNAME}:{RABBITMQ_PASSWORD}@localhost"
        )

        async with connection:
            # Creating a channel
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)

            # Declaring queue
            queue = await channel.declare_queue(
                self._id,
                durable=True,
            )

            # Start listening the queue with name 'task_queue'
            await queue.consume(self.on_message)

            print(" [*] Waiting for messages. To exit press CTRL+C")
            await asyncio.Future()

    async def on_message(self, message: AbstractIncomingMessage) -> None:
        async with message.process():
            await self.send(message.body.decode())

    def run(self):
        loop = asyncio.get_event_loop()
        while True:
            try:
                cors = asyncio.gather(
                    self.produce(f"SUBSCRIBE: {self._id}".encode()),
                    self.consume(),
                    self.connect(),
                    self.infinite_read_chat(),
                )
                loop.run_until_complete(cors)
            except Exception as exp:
                print("Error")
                continue
        loop.close()


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    # args.add_argument("--channels", type=str, default="")
    # channels = args.parse_args().channels.split(",")
    args.add_argument("--num", type=int, default=0)
    args.add_argument("--count", type=int, default=100)
    import pickle

    data = pickle.load(open("channels.pkl", "rb"))
    channels = data[
        args.parse_args().num : args.parse_args().num + args.parse_args().count
    ]
    subscriber = Subscriber(channels=channels)
    subscriber.run()
