import os
import pickle

import twitch
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

if __name__ == "__main__":
    client = twitch.TwitchHelix(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=[twitch.constants.OAUTH_SCOPE_ANALYTICS_READ_EXTENSIONS],
    )
    client.get_oauth()

    elems = client.get_streams(page_size=100)
    channels = []
    for i, elem in enumerate(elems):
        print(i)
        channels.append("#" + elem["user_login"])
        if len(channels) > 10000:
            break

    pickle.dump(channels, open("channels.pkl", "wb"))
