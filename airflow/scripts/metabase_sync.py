#!/usr/bin/env python3
"""Sync Metabase models after Spark pipeline completes."""

import json
import os
import sys
from urllib.request import Request, urlopen
from urllib.error import URLError

METABASE_URL = os.getenv("METABASE_URL", "http://metabase:3000")
METABASE_USER = os.getenv("METABASE_USER")
METABASE_PASS = os.getenv("METABASE_PASS")
DATABASE_ID = 3  # SparkSQL database in Metabase


def metabase_post(path, data, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Metabase-Session"] = token
    req = Request(f"{METABASE_URL}/api/{path}", data=json.dumps(data).encode(), headers=headers)
    try:
        with urlopen(req) as resp:
            return json.loads(resp.read())
    except URLError as e:
        print(f"Error: {e}")
        if hasattr(e, 'read'):
            print(e.read().decode())
        return None


def metabase_get(path, token):
    req = Request(f"{METABASE_URL}/api/{path}", headers={"X-Metabase-Session": token})
    with urlopen(req) as resp:
        return json.loads(resp.read())


def metabase_put(path, data, token):
    req = Request(f"{METABASE_URL}/api/{path}", data=json.dumps(data).encode(),
                  headers={"Content-Type": "application/json", "X-Metabase-Session": token})
    req.method = "PUT"
    with urlopen(req) as resp:
        return json.loads(resp.read())


def get_token():
    resp = metabase_post("session", {"username": METABASE_USER, "password": METABASE_PASS})
    return resp["id"] if resp else None


def find_or_create_model(token, name, query, display_name=None):
    """Find existing model by name or create a new one."""
    cards = metabase_get("card", token)
    if cards:
        for card in cards:
            if card.get("name") == name and card.get("type") == "model":
                print(f"  Updating existing model: {name}")
                # Update the query
                metabase_put(f"card/{card['id']}", {
                    "dataset_query": {
                        "database": DATABASE_ID,
                        "type": "native",
                        "native": {"query": query}
                    }
                }, token)
                return card["id"]

    print(f"  Creating new model: {name}")
    resp = metabase_post("card", {
        "name": name,
        "display": "table",
        "type": "model",
        "dataset_query": {
            "database": DATABASE_ID,
            "type": "native",
            "native": {"query": query}
        },
        "visualization_settings": {}
    }, token)
    return resp["id"] if resp else None


def create_or_update_dashboard(token, name, cards):
    """Create or update dashboard with given cards."""
    dashboards = metabase_get("dashboard", token)
    dashboard_id = None

    if dashboards:
        for d in dashboards:
            if d.get("name") == name:
                dashboard_id = d["id"]
                print(f"  Found existing dashboard: {name} (id={dashboard_id})")
                break

    if not dashboard_id:
        resp = metabase_post("dashboard", {"name": name, "description": "Auto-generated dashboard"}, token)
        if resp:
            dashboard_id = resp["id"]
            print(f"  Created new dashboard: {name} (id={dashboard_id})")

    if dashboard_id:
        # Build dashcards array
        dashcards = []
        for i, card_id in enumerate(cards):
            col = (i % 2) * 6
            row = (i // 2) * 6
            dashcards.append({
                "id": -1,
                "card_id": card_id,
                "row": row,
                "col": col,
                "size_x": 6,
                "size_y": 6,
                "parameter_mappings": [],
                "visualization_settings": {}
            })

        # First remove all existing dashcards
        existing = metabase_get(f"dashboard/{dashboard_id}", token)
        if existing and existing.get("dashcards"):
            existing_cards = [{"id": dc["id"], "card_id": dc["card_id"],
                              "row": dc["row"], "col": dc["col"],
                              "size_x": dc["size_x"], "size_y": dc["size_y"],
                              "parameter_mappings": dc.get("parameter_mappings", []),
                              "visualization_settings": dc.get("visualization_settings", {})}
                             for dc in existing["dashcards"]]
            metabase_put(f"dashboard/{dashboard_id}",
                        {"dashcards": existing_cards + dashcards}, token)
        else:
            metabase_put(f"dashboard/{dashboard_id}",
                        {"dashcards": dashcards}, token)

    return dashboard_id


def main():
    print("=== Metabase Sync ===")

    token = get_token()
    if not token:
        print("ERROR: Could not authenticate to Metabase")
        sys.exit(1)
    print("Authenticated to Metabase")

    # Define models to create
    models = [
        {
            "name": "faculty_profiles_model",
            "query": "SELECT source_system, full_name, title, department, email, faculty, research_interests, orcid_id, crawl_timestamp FROM curated.faculty_profiles"
        },
        {
            "name": "course_catalog_model",
            "query": "SELECT source_system, course_code, course_name, description, instructor, department, faculty, level, credits, language FROM curated.course_catalog"
        },
    ]

    model_ids = []
    for m in models:
        print(f"\nProcessing model: {m['name']}")
        card_id = find_or_create_model(token, m["name"], m["query"])
        if card_id:
            model_ids.append(card_id)
            print(f"  OK: {m['name']} (id={card_id})")

    # Create/update dashboard
    if model_ids:
        print("\n=== Dashboard ===")
        create_or_update_dashboard(token, "University Data Platform", model_ids)
        print("Dashboard updated")

    print("\n=== Sync Complete ===")


if __name__ == "__main__":
    main()
