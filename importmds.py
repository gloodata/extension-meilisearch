import os
import argparse
import pathlib

import toml
import yaml
import meilisearch
from slugify import slugify
import frontmatter

from marko import Markdown
from marko.block import Document, Heading, FencedCode
from marko.ext.gfm import GFM
from marko.md_renderer import MarkdownRenderer


def ensure_index_exists(client, index_uid, primary_key="id"):
    try:
        client.get_index(index_uid)
    except meilisearch.errors.MeilisearchApiError as err:
        if err.code == "index_not_found":
            client.create_index(index_uid, {"primaryKey": primary_key})
        else:
            raise err

    return client.index(index_uid)


def make_client_and_index(url, master_key, index_name):
    client = meilisearch.Client(url, master_key)
    index = ensure_index_exists(client, index_name)
    return client, index


def get_client_and_index(url, master_key, index_name):
    client = meilisearch.Client(url, master_key)
    index = client.index(index_name)
    return client, index


def parse_args():
    parser = argparse.ArgumentParser(description="Process markdown files.")
    parser.add_argument("base_path", help="Root path for glob pattern")
    parser.add_argument("glob_pattern", help="Glob pattern to match markdown files")
    return parser.parse_args()


class Item:
    def __init__(self, title, body, metadata):
        self.title = title
        self.body = body
        self.metadata = metadata


class ItemGroup:
    def __init__(self, path, items):
        self.path = path
        self.items = items


def items_to_md(md, childs):
    doc = Document()
    doc.children = childs
    return md.render(doc)


def process_file(md, file_path):
    items = []
    title = None
    metadata = None
    body = []
    with open(file_path, "r", encoding="utf-8") as file:
        full_content = file.read()
        base_metadata, content = frontmatter.parse(full_content)

        doc = md.parse(content)
        for node in doc.children:
            if isinstance(node, Heading) and node.level == 1:
                if title:
                    item = Item(title, items_to_md(md, body), metadata)
                    items.append(item)

                metadata = dict(base_metadata)
                title = items_to_md(md, node.children)
                body = []
            else:
                if (
                    isinstance(node, FencedCode)
                    and node.lang == "toml"
                    and node.extra == "metadata"
                ):
                    metadata.update(toml.loads(node.children[0].children))
                elif (
                    isinstance(node, FencedCode)
                    and node.lang == "yaml"
                    and node.extra == "metadata"
                ):
                    metadata.update(yaml.safe_load(node.children[0].children))
                else:
                    body.append(node)

        if title:
            item = Item(title, items_to_md(md, body), metadata)
            items.append(item)

    return items


def process_files(base_path, glob_pattern):
    md = Markdown(extensions=[GFM], renderer=MarkdownRenderer)
    groups = []
    for file_path in pathlib.Path(base_path).glob(glob_pattern):
        items = process_file(md, file_path)
        group = ItemGroup(file_path, items)
        groups.append(group)

    return groups


if __name__ == "__main__":
    ms_url = os.environ.get("MS_URL", "http://127.0.0.1:7700")
    ms_key = os.environ.get("MS_MASTER_KEY", "")
    ms_index = os.environ.get("MS_INDEX_NAME", "SearchIndex")

    client, index = make_client_and_index(ms_url, ms_key, ms_index)

    args = parse_args()
    groups = process_files(args.base_path, args.glob_pattern)
    entries = []
    for group in groups:
        file_name = os.path.relpath(group.path, args.base_path)
        print(f"File: {file_name}")
        for item in group.items:
            id = slugify(f"{file_name}:{item.title}")
            entry = dict(item.metadata)
            entry.update(id=id, title=item.title, body=item.body)
            entries.append(entry)

    print(f"inserting {len(entries)} entries")
    print(index.add_documents(entries))
