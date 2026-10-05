"""Storage abstraction supporting MongoDB and a deterministic local JSON fallback."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable

from config.settings import MONGO_DB, MONGO_TIMEOUT_MS, MONGO_URI, PROCESSED_DIR


class JsonStorage:
    backend = "json_fallback"

    def __init__(self, root: Path = PROCESSED_DIR) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.collections: dict[str, dict[str, dict[str, Any]]] = {}
        self._load()

    def _path(self, name: str) -> Path:
        return self.root / f"{name}.json"

    def _load(self) -> None:
        for name in ("orders_raw", "orders_validated", "orders_quarantine"):
            path = self._path(name)
            rows = []
            if path.exists():
                try:
                    with path.open("r", encoding="utf-8") as f:
                        first_char = f.read(1)
                        f.seek(0)
                        if first_char == "[":
                            rows = json.load(f)
                        else:
                            for line in f:
                                line = line.strip()
                                if line:
                                    rows.append(json.loads(line))
                except Exception:
                    rows = []
            key_name = "order_id" if name == "orders_validated" else "_storage_key"
            self.collections[name] = {str(row.get(key_name, i)): row for i, row in enumerate(rows)}

    def _flush(self, name: str) -> None:
        rows = list(self.collections.get(name, {}).values())
        path = self._path(name)
        with path.open("w", encoding="utf-8") as f:
            if len(rows) <= 2000:
                f.write(json.dumps(rows, ensure_ascii=False, indent=2, default=str))
            else:
                for row in rows:
                    f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

    def insert(self, collection: str, key: str, document: dict[str, Any]) -> str:
        bucket = self.collections.setdefault(collection, {})
        key_str = str(key)
        if key_str in bucket:
            return "unchanged"
        doc = dict(document)
        doc["_storage_key"] = key_str
        bucket[key_str] = doc
        return "inserted"

    def insert_many(self, collection: str, documents: list[dict[str, Any]]) -> int:
        bucket = self.collections.setdefault(collection, {})
        inserted = 0
        for doc in documents:
            key_name = "_storage_key"
            key_val = str(doc.get(key_name, f"{doc.get('run_id', 'run')}:{doc.get('source_row_number', len(bucket))}"))
            doc_copy = dict(doc)
            doc_copy["_storage_key"] = key_val
            bucket[key_val] = doc_copy
            inserted += 1
        return inserted

    def upsert(self, collection: str, key: str, document: dict[str, Any]) -> str:
        bucket = self.collections.setdefault(collection, {})
        key_str = str(key)
        doc = dict(document)
        field = "order_id" if collection == "orders_validated" else "_storage_key"
        doc[field] = key_str

        if key_str not in bucket:
            bucket[key_str] = doc
            return "inserted"

        # Compare existing document excluding non-deterministic audit timestamps
        existing = dict(bucket[key_str])
        cmp_existing = {k: v for k, v in existing.items() if k not in ("processed_at", "ingested_at", "run_id")}
        cmp_new = {k: v for k, v in doc.items() if k not in ("processed_at", "ingested_at", "run_id")}

        if cmp_existing == cmp_new:
            return "unchanged"

        bucket[key_str] = doc
        return "updated"

    def count(self, collection: str) -> int:
        return len(self.collections.get(collection, {}))

    def find_one(self, collection: str, key: str) -> dict[str, Any] | None:
        field = "order_id" if collection == "orders_validated" else "_storage_key"
        bucket = self.collections.get(collection, {})
        if str(key) in bucket:
            return bucket[str(key)]
        for doc in bucket.values():
            if str(doc.get(field)) == str(key):
                return doc
        return None

    def flush(self) -> None:
        for name in list(self.collections.keys()):
            self._flush(name)

    def close(self) -> None:
        self.flush()


class MongoStorage:
    backend = "mongodb"

    def __init__(self, client: Any, db: Any) -> None:
        self.client = client
        self.db = db
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        # Section 6.9: orders_raw MUST NOT have unique indexes or validators preventing loading
        self.db.orders_raw.create_index([("run_id", 1), ("source_row_number", 1)], unique=False)
        # Section 6.9 & 6.10: orders_validated MUST have Unique Index on order_id
        self.db.orders_validated.create_index("order_id", unique=True)
        # orders_quarantine indexed for querying
        self.db.orders_quarantine.create_index([("run_id", 1), ("source_row_number", 1)], unique=False)
        self.db.orders_quarantine.create_index("order_id", unique=False)

    def insert(self, collection: str, key: str, document: dict[str, Any]) -> str:
        doc = dict(document)
        doc["_storage_key"] = str(key)
        try:
            self.db[collection].insert_one(doc)
            return "inserted"
        except Exception as exc:
            if exc.__class__.__name__ == "DuplicateKeyError":
                return "unchanged"
            raise

    def insert_many(self, collection: str, documents: list[dict[str, Any]]) -> int:
        if not documents:
            return 0
        docs = []
        for doc in documents:
            d = dict(doc)
            if "_storage_key" not in d:
                d["_storage_key"] = f"{d.get('run_id', 'run')}:{d.get('source_row_number', 0)}"
            docs.append(d)
        result = self.db[collection].insert_many(docs, ordered=False)
        return len(result.inserted_ids)

    def upsert(self, collection: str, key: str, document: dict[str, Any]) -> str:
        field = "order_id" if collection == "orders_validated" else "_storage_key"
        doc = dict(document)
        doc[field] = str(key)

        existing = self.db[collection].find_one({field: str(key)})
        if existing is None:
            self.db[collection].replace_one({field: str(key)}, doc, upsert=True)
            return "inserted"

        cmp_existing = {k: v for k, v in existing.items() if k not in ("_id", "processed_at", "ingested_at", "run_id")}
        cmp_new = {k: v for k, v in doc.items() if k not in ("_id", "processed_at", "ingested_at", "run_id")}

        if cmp_existing == cmp_new:
            return "unchanged"

        self.db[collection].replace_one({field: str(key)}, doc, upsert=True)
        return "updated"

    def count(self, collection: str) -> int:
        return self.db[collection].count_documents({})

    def find_one(self, collection: str, key: str) -> dict[str, Any] | None:
        field = "order_id" if collection == "orders_validated" else "_storage_key"
        return self.db[collection].find_one({field: str(key)})

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass


def get_storage() -> JsonStorage | MongoStorage:
    requested = os.getenv("STORAGE_BACKEND", "auto").lower()
    if requested == "json":
        return JsonStorage()
    try:
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)
        client.admin.command("ping")
        return MongoStorage(client, client[MONGO_DB])
    except Exception:
        if requested == "mongo":
            raise RuntimeError("STORAGE_BACKEND=mongo but MongoDB is not reachable on localhost:27017")
        return JsonStorage()


Storage = JsonStorage | MongoStorage

