import asyncio
import datetime
import os

import twitch
from dotenv import load_dotenv
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

CLIENT = twitch.TwitchHelix(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    scopes=[twitch.constants.OAUTH_SCOPE_ANALYTICS_READ_EXTENSIONS],
)
CLIENT.get_oauth()


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


class Stream(Base):
    __tablename__ = "stream"

    id = Column(Integer, primary_key=True)
    user_login = Column(String)
    game_id = Column(String)
    timestamp = Column(DateTime)
    title = Column(String)
    started_at = Column(DateTime)
    language = Column(String)
    viewer_count = Column(Integer)

    # required in order to access columns with server defaults
    # or SQL expression defaults, subsequent to a flush, without
    # triggering an expired load
    __mapper_args__ = {"eager_defaults": True}


async def get_channels():
    channels = []
    for i, elem in enumerate(CLIENT.get_streams(page_size=100)):
        print(i)
        dict_elem = {
            k: v
            for k, v in elem.items()
            if k
            in (
                "user_login",
                "game_id",
                "title",
                "started_at",
                "language",
                "viewer_count",
            )
        }
        dict_elem["timestamp"] = datetime.datetime.utcnow()
        channels.append(Stream(**dict_elem))
        if len(channels) > 10000:
            break
    print("pushing to db")
    async with async_session() as session:
        async with session.begin():
            session.add_all(channels)
            await session.commit()
    return


async def infinite_get_channels():
    while True:
        await get_channels()
        await asyncio.sleep(60 * 10)


def main():
    loop = asyncio.get_event_loop()
    task_db = loop.create_task(create_db())
    loop.run_until_complete(task_db)
    task = loop.create_task(infinite_get_channels())
    loop.run_until_complete(task)


if __name__ == "__main__":
    main()
