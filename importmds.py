import argparse
import glob
import pathlib

import frontmatter
from marko import Markdown
from marko.block import Document, Heading
from marko.ext.gfm import GFM
from marko.md_renderer import MarkdownRenderer


def parse_args():
    parser = argparse.ArgumentParser(description="Process markdown files.")
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
    body = []
    with open(file_path, "r", encoding="utf-8") as file:
        full_content = file.read()
        metadata, content = frontmatter.parse(full_content)
        # Process the markdown content
        doc = md.parse(content)
        for node in doc.children:
            if isinstance(node, Heading) and node.level == 1:
                if title:
                    item = Item(title, items_to_md(md, body), metadata)
                    items.append(item)
                title = items_to_md(md, node.children)
                body = []
            else:
                body.append(node)

        if title:
            item = Item(title, items_to_md(md, body), metadata)
            items.append(item)

    return items


def process_files(glob_pattern):
    md = Markdown(extensions=[GFM], renderer=MarkdownRenderer)
    groups = []
    for file_path in glob.glob(glob_pattern):
        items = process_file(md, file_path)
        group = ItemGroup(file_path, items)
        groups.append(group)

    return groups


if __name__ == "__main__":
    args = parse_args()
    groups = process_files(args.glob_pattern)
    for group in groups:
        file_base_name = pathlib.Path(group.path).stem
        print(f"File: {file_base_name}")
        for item in group.items:
            print("-" * 40)
            print(f"Title: {item.title}")
            print(f"Body: {item.body}")
            print(f"Metadata: {item.metadata}")
