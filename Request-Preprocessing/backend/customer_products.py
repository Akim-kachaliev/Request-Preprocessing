from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection

from .config import Settings, get_settings

SUPPORTED_ENCODINGS = ("utf-8-sig", "utf-8", "cp1251", "cp866")


@dataclass(frozen=True)
class CustomerProductsImportSummary:
    files_processed: int
    source_records: int
    distinct_products: int
    merged_duplicates: int
    inserted: int
    updated: int
    collection_documents: int

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


def import_customer_products(
    file_paths: Iterable[str | Path],
    settings: Settings | None = None,
) -> CustomerProductsImportSummary:
    settings = settings or get_settings()
    resolved_paths = [Path(path).expanduser().resolve() for path in file_paths]
    if not resolved_paths:
        raise ValueError("No source files provided for customer products import")

    merged_docs, source_records = build_customer_products_documents(resolved_paths)

    with MongoClient(settings.mongo_uri) as client:
        collection = _get_collection(client, settings)
        _ensure_indexes(collection)

        existing_keys = {
            doc["nomenclature_normalized"]
            for doc in collection.find(
                {"nomenclature_normalized": {"$in": list(merged_docs)}},
                {"_id": 0, "nomenclature_normalized": 1},
            )
        }

        operations = [
            UpdateOne(
                {"nomenclature_normalized": key},
                {"$set": document},
                upsert=True,
            )
            for key, document in merged_docs.items()
        ]

        if operations:
            collection.bulk_write(operations, ordered=False)

        inserted = len(merged_docs) - len(existing_keys)
        updated = len(existing_keys)

        return CustomerProductsImportSummary(
            files_processed=len(resolved_paths),
            source_records=source_records,
            distinct_products=len(merged_docs),
            merged_duplicates=source_records - len(merged_docs),
            inserted=inserted,
            updated=updated,
            collection_documents=collection.count_documents({}),
        )


def find_customer_product(
    nomenclature: str,
    settings: Settings | None = None,
) -> dict[str, Any] | None:
    settings = settings or get_settings()
    normalized = normalize_nomenclature(nomenclature)

    with MongoClient(settings.mongo_uri) as client:
        collection = _get_collection(client, settings)
        doc = collection.find_one(
            {"nomenclature_normalized": normalized},
            {"_id": 0},
        )

    return doc


def get_customer_product_parameters(
    nomenclature: str,
    settings: Settings | None = None,
) -> dict[str, Any] | None:
    doc = find_customer_product(nomenclature, settings=settings)
    if doc is None:
        return None
    return doc.get("parameters")


def build_customer_products_documents(
    file_paths: Iterable[Path],
) -> tuple[dict[str, dict[str, Any]], int]:
    merged: dict[str, dict[str, Any]] = {}
    source_records = 0
    imported_at = utc_now_iso()

    for file_path in file_paths:
        catalog = load_customer_products_file(file_path)
        for nomenclature, parameters in catalog.items():
            source_records += 1
            normalized = normalize_nomenclature(nomenclature)
            existing = merged.get(normalized)
            if existing is None:
                merged[normalized] = {
                    "nomenclature": nomenclature,
                    "nomenclature_normalized": normalized,
                    "aliases": [nomenclature],
                    "parameters": parameters,
                    "source_files": [file_path.name],
                    "last_source_file": file_path.name,
                    "imported_at": imported_at,
                    "updated_at": imported_at,
                }
                continue

            aliases = set(existing["aliases"])
            aliases.add(nomenclature)
            source_files = set(existing["source_files"])
            source_files.add(file_path.name)

            existing.update(
                {
                    "nomenclature": nomenclature,
                    "aliases": sorted(aliases),
                    "parameters": parameters,
                    "source_files": sorted(source_files),
                    "last_source_file": file_path.name,
                    "updated_at": imported_at,
                }
            )

    return merged, source_records


def load_customer_products_file(file_path: str | Path) -> dict[str, dict[str, Any]]:
    path = Path(file_path)
    last_error: Exception | None = None

    for encoding in SUPPORTED_ENCODINGS:
        try:
            raw_data = json.loads(path.read_text(encoding=encoding))
            if not isinstance(raw_data, dict):
                raise ValueError(f"Top-level JSON structure must be an object: {path}")
            return {
                str(nomenclature): attributes
                for nomenclature, attributes in raw_data.items()
                if isinstance(attributes, dict)
            }
        except Exception as exc:
            last_error = exc

    raise ValueError(f"Failed to read customer products file: {path}") from last_error


def normalize_nomenclature(value: str) -> str:
    return " ".join(value.casefold().split())


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_collection(client: MongoClient, settings: Settings) -> Collection:
    return client[settings.customer_products_db][settings.customer_products_collection]


def _ensure_indexes(collection: Collection) -> None:
    collection.create_index("nomenclature_normalized", unique=True)
    collection.create_index("nomenclature")
