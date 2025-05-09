import os
from datetime import date

import meilisearch

from glootil import Toolbox, DynEnum

from slackdb import SlackDB


class State:
    def __init__(self):
        self.client = None
        self.index = None

    def setup(self):
        ms_url = os.environ.get("MS_URL", "http://127.0.0.1:7700")
        ms_key = os.environ.get("MS_MASTER_KEY", "")
        ms_index = os.environ.get("MS_INDEX_NAME", "SlackTreads")

        slack_data_dir = os.environ.get("SLACK_DATA_DIR", "./slack_archive/")

        # it's a sync client but we are only load things at startup
        # or do simple queries that should be fast
        slack_db = SlackDB(slack_data_dir)
        slack_db.initialize()

        client = meilisearch.Client(ms_url, ms_key)
        index = client.index(ms_index)

        self.client = client
        self.index = index
        self.slack_db = slack_db

    def get_all_channels(self, as_dict=True):
        return self.slack_db.get_all_channels(as_dict)

    def get_all_users(self, as_dict=True):
        return self.slack_db.get_all_users(as_dict)

    def find_users_like(self, query, limit):
        return self.slack_db.find_users_like(query, limit)


tb = Toolbox("gd-meilisearch", "Meilisearch", "Hybrid Search Tools", state=State())


@tb.enum(icon="hashtag")
class Channel(DynEnum):
    @staticmethod
    def load(state: State):
        return state.get_all_channels(as_dict=False)


@tb.enum(icon="user")
class User(DynEnum):
    @staticmethod
    def search(state: State, query: str = "", limit: int = 100):
        return [
            (r["id"], r["display_name"]) for r in state.find_users_like(query, limit)
        ]


def hit_to_search_item(hit):
    date = hit.get("date")
    title = hit.get("title", "")
    body = hit.get("content", "")

    return {"type": "Post", "format": "md", "title": title, "body": body, "date": date}


@tb.task
def search_handler(state: State, query: str = ""):
    resp = state.index.search(query)
    hits = resp.get("hits", [])
    return [hit_to_search_item(hit) for hit in hits]


SEARCH_HANDLER_ID = tb.handler_id_for_task(search_handler)


@tb.tool(
    name="Search",
    args={
        "query": "Query",
        "date_start": "From",
        "date_end": "To",
        "channel": "Channel",
        "user": "User",
    },
    examples=["Search for something", "Search for something else"],
)
def search(
    state: State,
    date_start: date | None,
    date_end: date | None,
    channel: Channel | None,
    user: User | None,
    query: str = "",
):
    "Search in Meilisearch"
    print(date_start, date_end, channel, user, query)
    return {
        "type": "Search",
        "placeholder": "Search Query",
        "query": query,
        "searchType": "submit",
        "searchHandlerName": SEARCH_HANDLER_ID,
    }
