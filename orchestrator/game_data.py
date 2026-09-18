"""Word game rules over the generic schema-driven database service."""
import asyncio
import hashlib
import json
import math
import re
import secrets
import urllib.request
from datetime import UTC, datetime, timedelta

TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{32,128}$")
MATCH_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
MAX_SNAPSHOT_BYTES = 240 * 1024
MAX_HISTORY_MATCHES = 20
MAX_HISTORY_AGE_DAYS = 30
MAX_PLAYER_NAME_LENGTH = 64
MATCH_STATUSES = frozenset(("waiting", "active", "paused", "stopped", "completed"))


def elo(rating, opponent, score):
    return round(rating + 32 * (score - 1.0 / (1.0 + math.pow(10.0, (opponent - rating) / 400.0))))


def _json(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, sort_keys=True, allow_nan=False)


def _profile_id(value):
    if type(value) is not int or value <= 0: raise ValueError("profile_id must be a positive integer")
    return value


def _match_id(value):
    if not isinstance(value, str) or not MATCH_RE.fullmatch(value): raise ValueError("invalid match_id")
    return value


def _name(value):
    if value is None: return ""
    if not isinstance(value, str): raise ValueError("player name must be a string")
    return " ".join("".join(char for char in value if char.isprintable()).split())[:MAX_PLAYER_NAME_LENGTH]


def _state(payload):
    match_id = _match_id(payload.get("match_id"))
    expected, version = payload.get("expected_state_version"), payload.get("state_version")
    if type(expected) is not int or not 0 <= expected <= 2**63 - 1: raise ValueError("expected_state_version must be a non-negative integer")
    if type(version) is not int or not 0 <= version <= 2**63 - 1: raise ValueError("state_version must be a non-negative integer")
    status, snapshot = payload.get("status"), payload.get("snapshot")
    if status not in MATCH_STATUSES: raise ValueError("invalid match status")
    if not isinstance(snapshot, dict) or snapshot.get("snapshot_schema_version") != 1: raise ValueError("unsupported snapshot schema")
    if snapshot.get("status") != status or snapshot.get("match_id") != match_id or snapshot.get("state_version") != version: raise ValueError("snapshot does not match request")
    if version < expected or (version == expected and version != 0): raise ValueError("state_version must advance expected_state_version")
    if len(_json(snapshot).encode()) > MAX_SNAPSHOT_BYTES: raise ValueError("snapshot is too large")
    return match_id, expected, version, status, snapshot


def _final_state(value, match_id):
    if not isinstance(value, dict) or value.get("match_id") != match_id or value.get("status") != "completed" or not isinstance(value.get("state"), dict):
        raise ValueError("final_state must be the completed public match state")
    if any(field in value["state"] for field in ("hand", "hands", "pool")): raise ValueError("final_state contains private game data")
    if len(_json(value).encode()) > MAX_SNAPSHOT_BYTES: raise ValueError("final_state is too large")
    return value


def _summary(row, profile_id):
    first = int(row["player_1_profile_id"]) == profile_id
    own, other = ("player_1", "player_2") if first else ("player_2", "player_1")
    winner = row["winner_profile_id"]
    return {"match_id": row["match_id"], "own_player_id": own, "own_name": row.get(own + "_name", ""), "opponent_name": row.get(other + "_name", ""),
            "opponent_profile_id": int(row[other + "_profile_id"]), "own_score": int(row[own + "_score"]), "opponent_score": int(row[other + "_score"]),
            "result": "draw" if winner is None else ("win" if int(winner) == profile_id else "loss"),
            "rating_delta": int(row[own + "_rating_after"]) - int(row[own + "_rating_before"]), "finished_at": row["created_at"]}


class GenericDbClient:
    def __init__(self, url, token): self.url, self.token = url.rstrip("/"), token

    async def call(self, path, payload):
        body = _json(payload).encode()
        request = urllib.request.Request(self.url + path, body, {"Content-Type": "application/json", "Authorization": "Bearer " + self.token})
        def send():
            with urllib.request.urlopen(request, timeout=5) as response: return json.loads(response.read())
        return await asyncio.to_thread(send)


class GameData:
    def __init__(self, client):
        self.client = client
        # ponytail: global lock, DB/native transaction or per-profile locks if replicas/throughput matter.
        self.lock = asyncio.Lock()

    async def _read(self, table, fields, filters, order=None, limit=100):
        return (await self.client.call("/v1/read", {"table": table, "fields": fields, "filters": filters, "order": order or [], "limit": limit}))["rows"]

    async def _transaction(self, operations): return await self.client.call("/v1/transaction", {"operations": operations})

    async def resolve_profile(self, token):
        if token is not None and (not isinstance(token, str) or not TOKEN_RE.fullmatch(token)): raise ValueError("profile_token must be a URL-safe string of 32-128 characters")
        async with self.lock:
            if token:
                rows = await self._read("players", ["profile_id", "rating"], [{"field": "token_hash", "value": hashlib.sha256(token.encode()).hexdigest()}], limit=1)
                if rows: return {**rows[0], "profile_token": token, "created": False}
            while True:
                token = secrets.token_urlsafe(32)
                result = await self._transaction([{"op": "insert", "table": "players", "values": {"token_hash": hashlib.sha256(token.encode()).hexdigest()}}])
                if result.get("committed"):
                    profile_id = result["results"][0]["last_insert_id"]
                    return {"profile_id": profile_id, "rating": 1200, "profile_token": token, "created": True}

    async def _matches(self, profile_id, limit=MAX_HISTORY_MATCHES):
        cutoff = (datetime.now(UTC) - timedelta(days=MAX_HISTORY_AGE_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        fields = ["match_id", "player_1_profile_id", "player_1_name", "player_2_profile_id", "player_2_name", "player_1_score", "player_2_score", "winner_profile_id", "player_1_rating_before", "player_1_rating_after", "player_2_rating_before", "player_2_rating_after", "final_state", "created_at"]
        order = [{"field": "created_at", "direction": "desc"}, {"field": "match_id", "direction": "desc"}]
        left = await self._read("rating_matches", fields, [{"field": "player_1_profile_id", "value": profile_id}, {"field": "created_at", "op": "gte", "value": cutoff}], order, limit)
        right = await self._read("rating_matches", fields, [{"field": "player_2_profile_id", "value": profile_id}, {"field": "created_at", "op": "gte", "value": cutoff}], order, limit)
        return sorted({row["match_id"]: row for row in left + right}.values(), key=lambda row: (str(row["created_at"]), row["match_id"]), reverse=True)

    async def history(self, payload):
        profile_id = _profile_id(payload.get("profile_id"))
        return {"matches": [_summary(row, profile_id) for row in (await self._matches(profile_id))[:MAX_HISTORY_MATCHES]]}

    async def detail(self, payload):
        profile_id, match_id = _profile_id(payload.get("profile_id")), _match_id(payload.get("match_id"))
        rows = [row for row in await self._matches(profile_id, 500) if row["match_id"] == match_id]
        return {"found": False} if not rows else {"found": True, "match": _summary(rows[0], profile_id), "final_state": rows[0]["final_state"]}

    async def complete(self, payload):
        match_id = _match_id(payload.get("match_id")); rated = payload.get("rated", True)
        one, two = _profile_id(payload.get("player_1_profile_id")), _profile_id(payload.get("player_2_profile_id"))
        if one == two or type(rated) is not bool: raise ValueError("players must be distinct positive integers")
        score_one, score_two = payload.get("player_1_score"), payload.get("player_2_score")
        if type(score_one) is not int or type(score_two) is not int or max(abs(score_one), abs(score_two)) > 1_000_000: raise ValueError("scores must be integers between -1000000 and 1000000")
        winner = payload.get("winner_profile_id"); outcome = payload.get("outcome")
        if winner is not None and winner not in (one, two): raise ValueError("winner_profile_id must identify a player or be null")
        if outcome != ("draw" if winner is None else ("player_1" if winner == one else "player_2")): raise ValueError("outcome does not match winner_profile_id")
        final = _final_state(payload.get("final_state"), match_id); name_one, name_two = _name(payload.get("player_1_name")), _name(payload.get("player_2_name"))
        async with self.lock:
            existing = await self._read("rating_matches", ["match_id", "player_1_rating_before", "player_2_rating_before", "player_1_rating_after", "player_2_rating_after"], [{"field": "match_id", "value": match_id}], limit=1)
            if existing: return {"stored": False, "match": existing[0]}
            players = await self._read("players", ["profile_id", "rating"], [{"field": "profile_id", "value": one}], limit=1) + await self._read("players", ["profile_id", "rating"], [{"field": "profile_id", "value": two}], limit=1)
            ratings = {row["profile_id"]: row["rating"] for row in players}
            if len(ratings) != 2: raise ValueError("unknown player profile")
            before_one, before_two = ratings[one], ratings[two]; result_one = .5 if winner is None else float(winner == one)
            after_one, after_two = (elo(before_one, before_two, result_one), elo(before_two, before_one, 1 - result_one)) if rated else (before_one, before_two)
            values = {"match_id": match_id, "player_1_profile_id": one, "player_1_name": name_one, "player_2_profile_id": two, "player_2_name": name_two, "player_1_score": score_one, "player_2_score": score_two, "winner_profile_id": winner, "player_1_rating_before": before_one, "player_2_rating_before": before_two, "player_1_rating_after": after_one, "player_2_rating_after": after_two, "final_state": final}
            ops = []
            if rated and after_one != before_one:
                ops.append({"op": "cas", "table": "players", "values": {"rating": after_one}, "filters": [{"field": "profile_id", "value": one}], "compare": [{"field": "rating", "value": before_one}]})
            if rated and after_two != before_two:
                ops.append({"op": "cas", "table": "players", "values": {"rating": after_two}, "filters": [{"field": "profile_id", "value": two}], "compare": [{"field": "rating", "value": before_two}]})
            ops.append({"op": "insert", "table": "rating_matches", "values": values})
            result = await self._transaction(ops)
            if not result.get("committed"):
                existing = await self._read("rating_matches", ["match_id", "player_1_rating_before", "player_2_rating_before", "player_1_rating_after", "player_2_rating_after"], [{"field": "match_id", "value": match_id}], limit=1)
                return {"stored": False, "match": existing[0]} if existing else {"stored": False, "error": "version_conflict"}
            return {"stored": True, "match": {"match_id": match_id, "player_1_rating_before": before_one, "player_2_rating_before": before_two, "player_1_rating_after": after_one, "player_2_rating_after": after_two}}

    async def save_state(self, payload):
        match_id, expected, version, status, snapshot = _state(payload)
        async with self.lock:
            rows = await self._read("active_matches", ["state_version", "status", "snapshot"], [{"field": "match_id", "value": match_id}], limit=1)
            if not rows:
                if expected != 0: return {"stored": False, "error": "version_conflict", "current_state_version": None}
                result = await self._transaction([{"op": "insert", "table": "active_matches", "values": {"match_id": match_id, "state_version": version, "status": status, "snapshot": snapshot}}])
                return {"stored": True, "state_version": version} if result.get("committed") else {"stored": False, "error": "version_conflict", "current_state_version": None}
            row, current = rows[0], int(rows[0]["state_version"])
            if current == version:
                return {"stored": True, "state_version": version, "idempotent": True} if row["status"] == status and _json(row["snapshot"]) == _json(snapshot) else {"stored": False, "error": "version_conflict", "current_state_version": current}
            if current != expected: return {"stored": False, "error": "version_conflict", "current_state_version": current}
            result = await self._transaction([{"op": "cas", "table": "active_matches", "values": {"state_version": version, "status": status, "snapshot": snapshot}, "filters": [{"field": "match_id", "value": match_id}], "compare": [{"field": "state_version", "value": current}]}])
            return {"stored": True, "state_version": version} if result.get("committed") else {"stored": False, "error": "version_conflict", "current_state_version": current}

    async def load_state(self, payload):
        match_id = _match_id(payload.get("match_id")); rows = await self._read("active_matches", ["match_id", "state_version", "status", "snapshot"], [{"field": "match_id", "value": match_id}], limit=1)
        return {"found": False} if not rows else {"found": True, **rows[0]}

    async def delete_state(self, payload):
        match_id = _match_id(payload.get("match_id")); result = await self._transaction([{"op": "delete", "table": "active_matches", "filters": [{"field": "match_id", "value": match_id}]}])
        return {"deleted": bool(result.get("results", [{}])[0].get("affected"))}

    async def dispatch(self, path, payload):
        methods = {"/profiles/resolve": lambda: self.resolve_profile(payload.get("profile_token")), "/matches/complete": lambda: self.complete(payload), "/matches/history": lambda: self.history(payload), "/matches/detail": lambda: self.detail(payload), "/matches/state/save": lambda: self.save_state(payload), "/matches/state/load": lambda: self.load_state(payload), "/matches/state/delete": lambda: self.delete_state(payload)}
        if path not in methods: raise ValueError("not_found")
        return await methods[path]()
