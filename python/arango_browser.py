#!/usr/bin/env python3

import argparse
import base64
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request


def http(method, url, user, password, payload=None):
    headers = {"accept": "application/json"}
    data = None

    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["content-type"] = "application/json"

    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    headers["authorization"] = f"Basic {token}"

    request = urllib.request.Request(url, data=data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(request) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        if body:
            sys.stderr.write(body + "\n")
        raise SystemExit(1) from exc
    except urllib.error.URLError as exc:
        sys.stderr.write(str(exc) + "\n")
        raise SystemExit(1) from exc


def read_json_input(input_file):
    if input_file:
        with open(input_file, "r", encoding="utf-8") as handle:
            content = handle.read().strip()
    else:
        content = sys.stdin.read().strip()

    if not content:
        raise SystemExit("Missing JSON input")

    try:
        return json.loads(content)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON input: {exc}") from exc


def json_preview(document):
    return json.dumps(document, indent=2, ensure_ascii=False, sort_keys=True)


def wrap_document(document, database=None):
    return {
        "database": database,
        "id": document.get("_id"),
        "key": document.get("_key"),
        "collection": document.get("_id", "/").split("/", 1)[0],
        "document": document,
        "preview": json_preview(document),
    }


def split_document_id(document_id):
    if not document_id or "/" not in document_id:
        raise SystemExit(f"Invalid document id: {document_id}")
    collection, key = document_id.split("/", 1)
    if not collection or not key:
        raise SystemExit(f"Invalid document id: {document_id}")
    return collection, key


def document_url(database_base, collection, key):
    collection = urllib.parse.quote(collection, safe="")
    key = urllib.parse.quote(key, safe="")
    return f"{database_base}/_api/document/{collection}/{key}"


def collection_url(database_base, collection):
    collection = urllib.parse.quote(collection, safe="")
    return f"{database_base}/_api/collection/{collection}"


def get_document(database_base, user, password, collection, key):
    return http("GET", document_url(database_base, collection, key), user, password)


def get_document_by_id(database_base, user, password, document_id):
    collection, key = split_document_id(document_id)
    return get_document(database_base, user, password, collection, key)


def list_collection_names(database_base, user, password):
    data = http("GET", f"{database_base}/_api/collection", user, password)
    collections = []
    for item in data.get("result", []):
        if item.get("isSystem"):
            continue
        collections.append(item["name"])
    return sorted(collections)


def get_collection_count(database_base, user, password, collection):
    data = http("GET", f"{collection_url(database_base, collection)}/count", user, password)
    return data.get("count", 0)


def run_aql(database_base, user, password, query, bind_vars=None, batch_size=1000):
    payload = {
        "query": query,
        "batchSize": batch_size,
        "count": True,
    }
    if bind_vars:
        payload["bindVars"] = bind_vars

    data = http("POST", f"{database_base}/_api/cursor", user, password, payload)

    result = list(data.get("result", []))
    extra = data.get("extra")
    cursor_id = data.get("id")

    while data.get("hasMore") and cursor_id:
        data = http("PUT", f"{database_base}/_api/cursor/{cursor_id}", user, password)
        result.extend(data.get("result", []))
        if extra is None:
            extra = data.get("extra")
        cursor_id = data.get("id", cursor_id)

    return {
        "result": result,
        "count": len(result),
        "extra": extra,
    }


def truncate_text(value, max_length=120):
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, sort_keys=True)
    text = text.replace("\n", " ").strip()
    if len(text) <= max_length:
        return text
    return text[: max_length - 1] + "..."


def extract_value(document, field_path):
    if not field_path or field_path == "*":
        return document

    value = document
    for part in field_path.split("."):
        if not part:
            continue
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def field_expression(field_path):
    if not field_path or field_path == "*":
        return "doc"

    expression = "doc"
    for part in field_path.split("."):
        if not part:
            continue
        if not re.fullmatch(r"[^.\[\]]+", part):
            raise SystemExit(f"Invalid field path segment: {part}")
        expression += f"[{json.dumps(part, ensure_ascii=False)}]"
    return expression


def collect_field_paths(value, prefix="", result=None, depth=0, max_depth=4):
    result = result if result is not None else set()
    if depth >= max_depth or not isinstance(value, dict):
        return result

    for key, nested in value.items():
        path = f"{prefix}.{key}" if prefix else key
        result.add(path)
        if isinstance(nested, dict):
            collect_field_paths(nested, path, result, depth + 1, max_depth)
    return result


def list_databases(server, user, password):
    data = http("GET", f"{server}/_db/_system/_api/database/user", user, password)
    for name in sorted(data.get("result", [])):
        print(name)


def list_collections(database_base, user, password):
    for name in list_collection_names(database_base, user, password):
        print(name)


def list_fields(database_base, user, password, collection, sample_size):
    query = "FOR doc IN @@collection LIMIT @sample RETURN doc"
    bind_vars = {
        "@collection": collection,
        "sample": sample_size,
    }
    data = run_aql(database_base, user, password, query, bind_vars)
    fields = {"_key", "_id", "_rev"}
    for document in data.get("result", []):
        collect_field_paths(document, result=fields)

    for field in sorted(fields):
        print(field)


def fetch_document(database_base, user, password, document_id):
    document = get_document_by_id(database_base, user, password, document_id)
    json.dump(
        wrap_document(document, urllib.parse.unquote(database_base.rsplit("/", 1)[-1])),
        sys.stdout,
        indent=2,
        ensure_ascii=False,
    )
    sys.stdout.write("\n")


def save_document(database_base, user, password, document):
    if not isinstance(document, dict):
        raise SystemExit("Document payload must be a JSON object")
    document_id = document.get("_id")
    if not document_id:
        raise SystemExit("Document payload must contain _id")

    collection, key = split_document_id(document_id)
    saved = http(
        "PUT",
        document_url(database_base, collection, key),
        user,
        password,
        document,
    )
    output = {
        "database": urllib.parse.unquote(database_base.rsplit("/", 1)[-1]),
        "id": document_id,
        "key": key,
        "collection": collection,
        "meta": saved,
        "document": get_document(database_base, user, password, collection, key),
    }
    output["preview"] = json_preview(output["document"])
    json.dump(output, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")


def delete_document(database_base, user, password, document_id):
    collection, key = split_document_id(document_id)
    deleted = http("DELETE", document_url(database_base, collection, key), user, password)
    output = {
        "database": urllib.parse.unquote(database_base.rsplit("/", 1)[-1]),
        "id": document_id,
        "key": key,
        "collection": collection,
        "meta": deleted,
    }
    json.dump(output, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")


def truncate_collection(database_base, user, password, collection):
    truncated = http(
        "PUT",
        f"{collection_url(database_base, collection)}/truncate?compact=false",
        user,
        password,
    )
    output = {
        "database": urllib.parse.unquote(database_base.rsplit("/", 1)[-1]),
        "name": truncated.get("name", collection),
        "collection": truncated,
    }
    json.dump(output, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")


def rename_collection(database_base, user, password, collection, new_name):
    new_name = (new_name or "").strip()
    if not new_name:
        raise SystemExit("Missing new collection name")
    if new_name == collection:
        raise SystemExit("The new collection name must be different")

    renamed = http(
        "PUT",
        f"{collection_url(database_base, collection)}/rename",
        user,
        password,
        {"name": new_name},
    )
    output = {
        "database": urllib.parse.unquote(database_base.rsplit("/", 1)[-1]),
        "old_name": collection,
        "name": renamed.get("name", new_name),
        "collection": renamed,
    }
    json.dump(output, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")


def search_related(database_base, user, password, field, value, limit):
    if not field:
        raise SystemExit("Missing field")
    expression = field_expression(field)
    candidates = []
    seen = set()
    query = "\n".join(
        [
            "FOR doc IN @@collection",
            f"LET related = {expression}",
            "FILTER (IS_ARRAY(related) AND POSITION(related, @value, true)) OR TO_STRING(related) == @value",
            "LIMIT @limit",
            "RETURN doc",
        ]
    )

    for collection in list_collection_names(database_base, user, password):
        result = run_aql(
            database_base,
            user,
            password,
            query,
            {"@collection": collection, "value": str(value), "limit": max(limit, 1)},
            batch_size=max(limit, 1),
        )
        for document in result.get("result", []):
            document_id = document.get("_id")
            if document_id and document_id not in seen:
                seen.add(document_id)
                candidates.append(wrap_document(document, urllib.parse.unquote(database_base.rsplit("/", 1)[-1])))

    json.dump({"matches": candidates}, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")


def browse_collection(database_base, user, password, collection, field, search, offset, limit):
    expression = field_expression(field)
    bind_vars = {
        "@collection": collection,
        "offset": max(offset, 0),
        "limit": max(limit, 1) + 1,
    }

    filters = []
    if search:
        bind_vars["search"] = search.lower()
        filters.append(f"FILTER CONTAINS(LOWER(TO_STRING({expression})), @search)")

    query_lines = [
        "FOR doc IN @@collection",
        *filters,
        "SORT doc._key",
        "LIMIT @offset, @limit",
        "RETURN doc",
    ]
    data = run_aql(database_base, user, password, "\n".join(query_lines), bind_vars)

    documents = data.get("result", [])
    has_more = len(documents) > limit
    documents = documents[:limit]
    total_count = get_collection_count(database_base, user, password, collection)

    items = []
    for document in documents:
        value = extract_value(document, field)
        items.append(
            {
                "key": document.get("_key"),
                "id": document.get("_id"),
                "field": field,
                "field_value": value,
                "field_value_text": truncate_text(value),
                "preview": json.dumps(document, indent=2, ensure_ascii=False, sort_keys=True),
            }
        )

    output = {
        "database": urllib.parse.unquote(database_base.rsplit("/", 1)[-1]),
        "collection": collection,
        "field": field,
        "search": search,
        "offset": max(offset, 0),
        "limit": max(limit, 1),
        "total_count": total_count,
        "has_more": has_more,
        "items": items,
    }
    json.dump(output, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", required=True)

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("collections")
    subparsers.add_parser("databases")
    fields_parser = subparsers.add_parser("fields")
    fields_parser.add_argument("--collection", required=True)
    fields_parser.add_argument("--sample-size", type=int, default=100)
    browse_parser = subparsers.add_parser("browse")
    browse_parser.add_argument("--collection", required=True)
    browse_parser.add_argument("--field", default="_key")
    browse_parser.add_argument("--search", default="")
    browse_parser.add_argument("--offset", type=int, default=0)
    browse_parser.add_argument("--limit", type=int, default=50)
    get_parser = subparsers.add_parser("get")
    get_parser.add_argument("--id", required=True)
    save_parser = subparsers.add_parser("save")
    save_parser.add_argument("--input-file")
    delete_parser = subparsers.add_parser("delete")
    delete_parser.add_argument("--id", required=True)
    rename_collection_parser = subparsers.add_parser("rename-collection")
    rename_collection_parser.add_argument("--collection", required=True)
    rename_collection_parser.add_argument("--name", required=True)
    truncate_collection_parser = subparsers.add_parser("truncate-collection")
    truncate_collection_parser.add_argument("--collection", required=True)
    search_related_parser = subparsers.add_parser("search-related")
    search_related_parser.add_argument("--field", required=True)
    search_related_parser.add_argument("--value", required=True)
    search_related_parser.add_argument("--limit", type=int, default=20)

    args = parser.parse_args()

    server = f"http://{args.host}:{args.port}"
    database = urllib.parse.quote(args.database, safe="")
    database_base = f"{server}/_db/{database}"

    if args.command == "databases":
        list_databases(server, args.user, args.password)
        return

    if args.command == "collections":
        list_collections(database_base, args.user, args.password)
        return

    if args.command == "fields":
        list_fields(database_base, args.user, args.password, args.collection, max(args.sample_size, 1))
        return

    if args.command == "browse":
        browse_collection(
            database_base,
            args.user,
            args.password,
            args.collection,
            args.field,
            args.search,
            args.offset,
            args.limit,
        )
        return

    if args.command == "get":
        fetch_document(database_base, args.user, args.password, args.id)
        return

    if args.command == "save":
        save_document(database_base, args.user, args.password, read_json_input(args.input_file))
        return

    if args.command == "delete":
        delete_document(database_base, args.user, args.password, args.id)
        return

    if args.command == "rename-collection":
        rename_collection(database_base, args.user, args.password, args.collection, args.name)
        return

    if args.command == "truncate-collection":
        truncate_collection(database_base, args.user, args.password, args.collection)
        return

    if args.command == "search-related":
        search_related(
            database_base,
            args.user,
            args.password,
            args.field,
            args.value,
            args.limit,
        )
        return


if __name__ == "__main__":
    main()
