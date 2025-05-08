import json
import sys
from pathlib import Path
from typing import Dict, List

import duckdb


class DuckStore:
    def __init__(self):
        self.conn = duckdb.connect(":memory:")

    def initialize(self):
        pass

    def query_one(self, query: str, params: dict) -> Dict | None:
        cursor = self.conn.sql(query, params=params)
        result = cursor.fetchone()
        if result:
            return {key: value for key, value in zip(cursor.columns, result)}
        return None

    def query_all(self, query: str, params: dict) -> List[Dict]:
        cursor = self.conn.sql(query, params=params)
        result = cursor.fetchall()
        cols = cursor.columns
        return [{key: value for key, value in zip(cols, row)} for row in result]

    def close(self):
        """Close the database connection."""
        self.conn.close()

    def __enter__(self):
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class SlackDB(DuckStore):
    def __init__(self, data_dir: str):
        super().__init__()
        self.data_dir = Path(data_dir)

    def initialize(self):
        channels_path = self.data_dir / "channels.json"
        with open(channels_path) as f:
            channels_data = json.load(f)

        # Create and populate channels table
        self.conn.execute("""
            CREATE TABLE channels (
                id VARCHAR,
                name VARCHAR,
                created BIGINT,
                is_archived BOOLEAN,
                is_general BOOLEAN,
                topic VARCHAR,
                purpose VARCHAR
            )
        """)

        for channel in channels_data:
            self.conn.execute(
                """
                INSERT INTO channels (id, name, created, is_archived, is_general, topic, purpose)
                VALUES ($id, $name, $created, $is_archived, $is_general, $topic, $purpose)
            """,
                {
                    "id": channel["id"],
                    "name": channel["name"],
                    "created": channel["created"],
                    "is_archived": channel.get("is_archived", False),
                    "is_general": channel.get("is_general", False),
                    "topic": channel.get("topic", {}).get("value", ""),
                    "purpose": channel.get("purpose", {}).get("value", ""),
                },
            )

        users_path = self.data_dir / "users.json"
        with open(users_path) as f:
            users_data = json.load(f)

        self.conn.execute("""
            CREATE TABLE users (
                id VARCHAR,
                name VARCHAR,
                real_name VARCHAR,
                display_name VARCHAR,
                is_bot BOOLEAN,
                is_deleted BOOLEAN,
                email VARCHAR
            )
        """)

        for user in users_data:
            self.conn.execute(
                """
                INSERT INTO users (id, name, real_name, display_name, is_bot, is_deleted, email)
                VALUES ($id, $name, $real_name, $display_name, $is_bot, $is_deleted, $email)
            """,
                {
                    "id": user["id"],
                    "name": user["name"],
                    "real_name": user.get("real_name", ""),
                    "display_name": user.get("profile", {}).get("display_name", ""),
                    "is_bot": user.get("is_bot", False),
                    "is_deleted": user.get("deleted", False),
                    "email": user.get("profile", {}).get("email", ""),
                },
            )

    def get_all_channels(self) -> List[Dict]:
        """Return a list of tuples containing channel IDs and names."""
        return self.query_all(
            """
            SELECT id, name
            FROM channels
            WHERE is_archived = false
            ORDER BY name
        """,
            {},
        )

    def get_all_users(self) -> List[Dict]:
        """Return a list of tuples containing user IDs and names."""
        return self.query_all(
            """
            SELECT id, COALESCE(display_name, real_name, name) as name
            FROM users
            WHERE is_deleted = false
            ORDER BY name
        """,
            {},
        )

    def get_channel_by_id(self, channel_id: str) -> Dict | None:
        """Get channel details by ID."""
        return self.query_one(
            """
            SELECT id, name, created, is_archived, is_general, topic, purpose
            FROM channels
            WHERE id = $channel_id
        """,
            {"channel_id": channel_id},
        )

    def get_user_by_id(self, user_id: str) -> Dict | None:
        """Get user details by ID."""
        return self.query_one(
            """
                    SELECT id, name, real_name, COALESCE(display_name, real_name, name) as display_name, is_bot, is_deleted, email
                    FROM users
                    WHERE id = $user_id
                """,
            {"user_id": user_id},
        )

    def find_users_like(self, query: str, limit: int = 100) -> List[Dict]:
        """Find users whose name matches query pattern.
        Uses fuzzy matching against name, real_name and display_name fields.
        Returns up to limit results ordered by closest match."""
        return self.query_all(
            """
            SELECT
                id,
                name,
                real_name,
                COALESCE(display_name, real_name, name) as display_name,
                is_bot,
                is_deleted,
                email
            FROM users
            WHERE (
                LOWER(name) LIKE '%' || LOWER($query) || '%'
                OR LOWER(real_name) LIKE '%' || LOWER($query) || '%'
                OR LOWER(display_name) LIKE '%' || LOWER($query) || '%'
            )
            AND is_deleted = false
            ORDER BY display_name, real_name, name
            LIMIT $limit
            """,
            {"query": query, "limit": limit},
        )

    def find_channels_like(self, query: str, limit: int = 100) -> List[Dict]:
        """Find channels whose name matches query pattern.
        Uses fuzzy matching against the channel name.
        Returns up to limit results ordered by closest match."""
        return self.query_all(
            """
            SELECT
                id,
                name,
                created,
                is_archived,
                is_general,
                topic,
                purpose
            FROM channels
            WHERE LOWER(name) LIKE '%' || LOWER($query) || '%'
            AND is_archived = false
            ORDER BY name
            LIMIT $limit
            """,
            {"query": query, "limit": limit},
        )


def main():
    import pprint

    if len(sys.argv) != 2:
        print("Usage: python slackdb.py <path_to_slack_export>")
        sys.exit(1)

    export_path = sys.argv[1]

    def print_channel(channel):
        channel_id = channel["id"]
        channel_name = channel["name"]
        print(f"{channel_name:20} (ID: {channel_id})")

    def print_user(user):
        user_id = user["id"]
        user_name = user.get("display_name") or user.get("real_name") or user["name"]
        print(f"{user_name:20} (ID: {user_id})")

    with SlackDB(export_path) as db:
        print("\nChannels:")
        print("---------")
        channels = db.get_all_channels()
        for channel in channels:
            print(channel)

        print("\nUsers:")
        print("------")
        users = db.get_all_users()
        for user in users:
            print_user(user)

        print("\nOne User:")
        pprint.pprint(db.get_user_by_id(users[0]["id"]))

        print("\nOne Group:")
        pprint.pprint(db.get_channel_by_id(channels[0]["id"]))

        print("\nFind Users:")
        for user in db.find_users_like("maria"):
            print_user(user)

        print("\nFind Groups:")
        for channel in db.find_channels_like("th"):
            print_channel(channel)


if __name__ == "__main__":
    main()
