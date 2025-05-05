import os

import meilisearch

from glootil import Toolbox


class State:
    def __init__(self):
        self.client = None
        self.index = None

    def setup(self):
        ms_url = os.environ.get("MS_URL", "http://127.0.0.1:7700")
        ms_key = os.environ.get("MS_MASTER_KEY", "")
        ms_index = os.environ.get("MS_INDEX_NAME", "SearchIndex")

        client = meilisearch.Client(ms_url, ms_key)
        index = client.index(ms_index)

        self.client = client
        self.index = index


tb = Toolbox("gd-meilisearch", "Meilisearch", "Hybrid Search Tools", state=State())


def hit_to_search_item(hit):
    title = hit.get("title", "")
    body = hit.get("body", "")
    return {"type": "Post", "title": title, "body": body}


@tb.task
def search_handler(state: State, query: str = ""):
    resp = state.index.search(query)
    hits = resp.get("hits", [])
    return [hit_to_search_item(hit) for hit in hits]


SEARCH_HANDLER_ID = tb.handler_id_for_task(search_handler)


@tb.tool(
    name="Search",
    args={"query": "Query"},
    examples=["Search for something", "Search for something else"],
)
def buscar_faq(query: str = ""):
    "Search in Meilisearch"
    return {
        "type": "Search",
        "placeholder": "Search Query",
        "query": query,
        "searchType": "submit",
        "searchHandlerName": SEARCH_HANDLER_ID,
    }


tb.serve(host="127.0.0.1", port=8089)
