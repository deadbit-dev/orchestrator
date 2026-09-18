import unittest
from unittest.mock import AsyncMock
from orchestrator.game_data import GameData
from service import Orchestrator


class FakeGeneric:
    def __init__(self):
        self.tables = {"players": [], "rating_matches": [], "active_matches": []}
        self.next_id = 1

    async def call(self, path, payload):
        if path == "/v1/read":
            rows = self.tables[payload["table"]]
            for item in payload.get("filters", []):
                op, value = item.get("op", "eq"), item["value"]
                rows = [row for row in rows if {"eq": row.get(item["field"]) == value, "gte": row.get(item["field"]) >= value}.get(op, False)]
            for order in reversed(payload.get("order", [])):
                rows = sorted(rows, key=lambda row: str(row[order["field"]]), reverse=order.get("direction") == "desc")
            return {"rows": [{field: row[field] for field in payload["fields"]} for row in rows[:payload["limit"]]]}
        assert path == "/v1/transaction"
        results = []
        for operation in payload["operations"]:
            table, rows, op = operation["table"], self.tables[operation["table"]], operation["op"]
            matches = [row for row in rows if all(row.get(item["field"]) == item["value"] for item in operation.get("filters", []))]
            if op == "insert":
                row = dict(operation["values"])
                if table == "players": row["profile_id"] = self.next_id; row["rating"] = 1200; self.next_id += 1
                rows.append(row); results.append({"affected": 1, "last_insert_id": row.get("profile_id", 0), "conflict": False})
            elif op == "delete":
                self.tables[table] = [row for row in rows if row not in matches]; results.append({"affected": len(matches), "conflict": False})
            else:
                compare = operation.get("compare", [])
                matches = [row for row in matches if all(row.get(item["field"]) == item["value"] for item in compare)]
                changed = [row for row in matches if any(row.get(field) != value for field, value in operation["values"].items())]
                for row in changed: row.update(operation["values"])
                results.append({"affected": len(changed), "conflict": op == "cas" and not changed})
        return {"committed": not any(result["conflict"] for result in results), "results": results}


class GameDataTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self): self.generic = FakeGeneric(); self.data = GameData(self.generic)

    async def test_profile_state_and_idempotency(self):
        profile = await self.data.resolve_profile(None)
        self.assertEqual((profile["profile_id"], profile["rating"]), (1, 1200))
        snapshot = {"snapshot_schema_version": 1, "match_id": "m", "status": "active", "state_version": 0, "state": {}}
        self.assertTrue((await self.data.save_state({"match_id": "m", "expected_state_version": 0, "state_version": 0, "status": "active", "snapshot": snapshot}))["stored"])
        self.assertTrue((await self.data.save_state({"match_id": "m", "expected_state_version": 0, "state_version": 0, "status": "active", "snapshot": snapshot}))["idempotent"])
        self.assertEqual((await self.data.save_state({"match_id": "m", "expected_state_version": 0, "state_version": 1, "status": "active", "snapshot": {**snapshot, "state_version": 1}}))["state_version"], 1)

    async def test_match_history_and_elo(self):
        one, two = await self.data.resolve_profile(None), await self.data.resolve_profile(None)
        final = {"match_id": "m", "status": "completed", "state": {}}
        stored = await self.data.complete({"match_id": "m", "rated": True, "player_1_profile_id": one["profile_id"], "player_2_profile_id": two["profile_id"], "player_1_name": "A", "player_2_name": "B", "player_1_score": 10, "player_2_score": 4, "winner_profile_id": one["profile_id"], "outcome": "player_1", "final_state": final})
        self.assertEqual(stored["match"]["player_1_rating_after"], 1216)
        self.generic.tables["rating_matches"][0]["created_at"] = "9999-01-01T00:00:00+00:00"
        history = await self.data.history({"profile_id": one["profile_id"]})
        self.assertEqual((history["matches"][0]["result"], history["matches"][0]["opponent_name"]), ("win", "B"))

    async def test_equal_rating_draw_does_not_cas_unchanged_values(self):
        one, two = await self.data.resolve_profile(None), await self.data.resolve_profile(None)
        result = await self.data.complete({"match_id": "draw", "rated": True, "player_1_profile_id": one["profile_id"], "player_2_profile_id": two["profile_id"], "player_1_score": 4, "player_2_score": 4, "winner_profile_id": None, "outcome": "draw", "final_state": {"match_id": "draw", "status": "completed", "state": {}}})
        self.assertTrue(result["stored"])

    async def test_orchestrator_domain_api_requires_server_bearer(self):
        app = Orchestrator("http://unused", "server-token")
        app.data.dispatch = AsyncMock(return_value={"found": False})
        body = b'{"match_id":"m"}'
        self.assertEqual((await app.api_response("POST", "/matches/state/load", {}, body))[0], "401 Unauthorized")
        status, value = await app.api_response("POST", "/matches/state/load", {"Authorization": "Bearer server-token"}, body)
        self.assertEqual((status, value), ("200 OK", {"found": False}))
        app.data.dispatch.assert_awaited_once_with("/matches/state/load", {"match_id": "m"})


if __name__ == "__main__": unittest.main()
