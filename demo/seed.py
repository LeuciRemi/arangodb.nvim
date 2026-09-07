#!/usr/bin/env python3
"""Seed only the dedicated local demo server; reruns replace fixture documents."""
import base64
import json
import time
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:18529"
AUTH = "Basic " + base64.b64encode(b"root:demo-only").decode()


def request(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data, {"Authorization": AUTH, "Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.load(response)


for attempt in range(30):
    try:
        request("GET", "/_api/version")
        break
    except (urllib.error.URLError, TimeoutError):
        time.sleep(1)
else:
    raise SystemExit("Demo server did not become ready on port 18529")

if "demo" not in request("GET", "/_api/database")["result"]:
    request("POST", "/_api/database", {"name": "demo"})

fixtures = {
    "users": [
        {"_key": "alice", "name": "Alice Martin", "role": "Engineer", "city": "Lyon", "active": True},
        {"_key": "ben", "name": "Ben Taylor", "role": "Designer", "city": "London", "active": True},
        {"_key": "chloe", "name": "Chloe Dubois", "role": "Engineer", "city": "Paris", "active": True},
        {"_key": "diego", "name": "Diego Garcia", "role": "Writer", "city": "Madrid", "active": False},
    ],
    "projects": [
        {"_key": "atlas", "name": "Atlas", "status": "in progress", "owner_id": "users/alice", "stars": 128},
        {"_key": "bloom", "name": "Bloom", "status": "released", "owner_id": "users/ben", "stars": 84},
        {"_key": "compass", "name": "Compass", "status": "planning", "owner_id": "users/chloe", "stars": 32},
    ],
    "contributes": [
        {"_key": "alice-atlas", "_from": "users/alice", "_to": "projects/atlas"},
        {"_key": "ben-atlas", "_from": "users/ben", "_to": "projects/atlas"},
        {"_key": "ben-bloom", "_from": "users/ben", "_to": "projects/bloom"},
        {"_key": "chloe-compass", "_from": "users/chloe", "_to": "projects/compass"},
    ],
}
existing = {c["name"] for c in request("GET", "/_db/demo/_api/collection")["result"]}
for name, documents in fixtures.items():
    if name not in existing:
        request("POST", "/_db/demo/_api/collection", {"name": name, "type": 3 if name == "contributes" else 2})
    result = request("POST", f"/_db/demo/_api/document/{name}?overwriteMode=replace", documents)
    assert all(not item.get("error") for item in result), result
if "team" not in {g["name"] for g in request("GET", "/_db/demo/_api/gharial")["graphs"]}:
    request("POST", "/_db/demo/_api/gharial", {"name": "team", "edgeDefinitions": [{"collection": "contributes", "from": ["users"], "to": ["projects"]}]})
print("Demo ready: 4 users, 3 projects, 4 edges, named graph 'team'.")
