#!/usr/bin/env python3
"""Small lobby/control-plane for Scrabble match servers."""
import argparse
import asyncio
import json
import os
import secrets
import time
import urllib.error
import urllib.request
import uuid
from orchestrator.queue import Queue
from orchestrator.servers import ServerRegistry
from orchestrator.app import serve_queue_api
from orchestrator.scrabble_data import GenericDbClient, ScrabbleData

VERSION = 6
MAX_MESSAGE_BYTES = 32 * 1024
MAX_TEXT = 128
MAX_PLAYER_NAME_LENGTH = 64
STALE_SERVER_SECONDS = 20
COMMAND_TIMEOUT_SECONDS = 8
PENDING_ASSIGNMENT_SECONDS = 300
MAX_PENDING_ASSIGNMENTS = 10000
DEFAULT_FALLBACK_SECONDS = 10.0


def envelope(message_type, request_id, payload, match_id=None):
    value = {"v": VERSION, "type": message_type, "request_id": request_id, "payload": payload}
    if match_id:
        value["match_id"] = match_id
    return value


def rating_gap(wait_seconds):
    return min(400, 100 + 50 * int(max(0.0, wait_seconds) // 15.0))


def player_name(value):
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError("player_name must be a string")
    return " ".join("".join(char for char in value if char.isprintable()).split())[:MAX_PLAYER_NAME_LENGTH]


class State:
    """Pure in-memory routing and matching state."""
    def __init__(self):
        self.registry, self.queue, self.invites = ServerRegistry(MAX_TEXT, STALE_SERVER_SECONDS), Queue(), {}

    @property
    def servers(self):
        return self.registry.servers

    @property
    def routes(self):
        return self.registry.routes

    def register(self, server_id, public_url, max_matches, match_ids=None, active_matches=None):
        return self.registry.register(server_id, public_url, max_matches, match_ids, active_matches)

    def heartbeat(self, server_id, match_ids, active_matches, max_matches=None):
        return self.registry.heartbeat(server_id, match_ids, active_matches, max_matches)

    def select_server(self, now=None):
        return self.registry.select_server(now)

    def release(self, server_id):
        self.registry.release(server_id)

    def route(self, match_id, server_hint=""):
        return self.registry.route(match_id, server_hint)

    def queue_player(self, item):
        self.queue.queue_player(item)

    def cancel_player(self, profile_id):
        self.queue.cancel_player(profile_id)

    def pairs(self, now=None, fallback_seconds=DEFAULT_FALLBACK_SECONDS):
        return self.queue.pairs(now, fallback_seconds)


class Orchestrator:
    def __init__(self, db_service_url, server_token, fallback_seconds=DEFAULT_FALLBACK_SECONDS, db_service_token=""):
        self.state, self.server_token = State(), server_token
        self.data = ScrabbleData(GenericDbClient(db_service_url, db_service_token))
        self.fallback_seconds = fallback_seconds
        self.clients, self.commands, self.matches, self.rematches = {}, {}, {}, {}
        self.pending_assignments, self.cancelled = {}, {}
        self.lock = asyncio.Lock()

    def prune_lifecycle(self):
        now = time.monotonic()
        self.pending_assignments = {profile_id: item for profile_id, item in self.pending_assignments.items() if item["expires_at"] > now}
        self.cancelled = {profile_id: expires_at for profile_id, expires_at in self.cancelled.items() if expires_at > now}

    def requeue_player(self, player):
        self.state.queue_player(player)

    async def assign(self, player, message, server_id, player_id):
        """Deliver an assignment; only the match server's join ACK completes it."""
        profile_id = player["profile_id"]
        if profile_id in self.cancelled:
            return False
        self.prune_lifecycle()
        if len(self.pending_assignments) >= MAX_PENDING_ASSIGNMENTS:
            self.pending_assignments.pop(next(iter(self.pending_assignments)), None)
        self.pending_assignments[profile_id] = {
            "message": message, "match_id": message["match_id"], "server_id": server_id,
            "player_id": player_id, "expires_at": time.monotonic() + PENDING_ASSIGNMENT_SECONDS,
        }
        client = self.clients.get(profile_id)
        try:
            if not client:
                raise ConnectionError("client unavailable")
            await self.send(client["websocket"], message)
        except Exception:
            return False
        return True

    async def resend_assignment(self, profile_id):
        self.prune_lifecycle()
        pending = self.pending_assignments.get(profile_id)
        client = self.clients.get(profile_id)
        if not pending or not client or profile_id in self.cancelled:
            return False
        try:
            await self.send(client["websocket"], pending["message"])
        except Exception:
            return False
        return True

    async def cancel_pair(self, profile_id, request_id):
        """Make an in-flight match harmless and return the other searcher to queue."""
        async with self.lock:
            self.prune_lifecycle()
            pending = self.pending_assignments.get(profile_id)
            if pending:
                client = self.clients.get(profile_id)
                if client:
                    await self.error(client["websocket"], request_id, "match_already_assigned", "Match assignment is awaiting server confirmation")
                return await self.resend_assignment(profile_id)
            self.cancelled[profile_id] = time.monotonic() + COMMAND_TIMEOUT_SECONDS + 1
            self.state.cancel_player(profile_id)
            size = len(self.state.queue.entries)
        client = self.clients.get(profile_id)
        if client:
            await self.send(client["websocket"], envelope("matchmaking_status", request_id, {"status": "cancelled", "queue_size": size}))

    async def resolve_profile(self, token):
        return await self.data.dispatch("/profiles/resolve", {"profile_token": token})

    async def proxy(self, path, payload):
        return await self.data.dispatch(path, payload)

    async def send(self, websocket, message):
        await websocket.send(json.dumps(message, separators=(",", ":"), ensure_ascii=False))

    async def error(self, websocket, request_id, code, message):
        await self.send(websocket, envelope("error", request_id, {"code": code, "message": message}))

    async def api_response(self, method, path, headers, body=b""):
        if method == "GET" and path == "/v1/matchmaking/queue":
            async with self.lock:
                return "200 OK", self.state.queue.snapshot()
        if method != "POST" or path not in ("/profiles/resolve", "/matches/complete", "/matches/history", "/matches/detail", "/matches/state/save", "/matches/state/load", "/matches/state/delete"):
            return "404 Not Found", {}
        authorization = next((value.strip() for key, value in headers.items() if key.lower() == "authorization"), "")
        if not secrets.compare_digest(authorization, "Bearer " + self.server_token):
            return "401 Unauthorized", {"error": "unauthorized"}
        try:
            payload = json.loads(body)
            if not isinstance(payload, dict): raise ValueError("body must be an object")
            return "200 OK", await self.data.dispatch(path, payload)
        except (ValueError, json.JSONDecodeError) as error:
            return "400 Bad Request", {"error": str(error)}
        except Exception:
            return "503 Service Unavailable", {"error": "data_store_unavailable"}

    def status(self):
        now = time.monotonic()
        servers = []
        for server in sorted(self.state.servers.values(), key=lambda item: item.server_id):
            seen_seconds = max(0.0, now - server.seen_at)
            used = server.active_matches + server.reservations
            servers.append({
                "server_id": server.server_id,
                "public_url": server.public_url,
                "online": server.websocket is not None and seen_seconds <= STALE_SERVER_SECONDS,
                "active_matches": server.active_matches,
                "reservations": server.reservations,
                "max_matches": server.max_matches,
                "load_percent": round(100 * used / server.max_matches, 1),
                "seen_seconds": round(seen_seconds, 1),
            })
        return {
            "clients": len(self.clients),
            "queue_size": len(self.state.queue.entries),
            "servers": servers,
        }

    async def admin_message(self, websocket, message):
        if (message["type"] != "admin_status"
                or not secrets.compare_digest(str(message["payload"].get("token", "")), self.server_token)):
            return False
        await self.send(websocket, envelope("admin_status", message["request_id"], self.status()))
        return True

    async def disconnect_client(self, client):
        profile_id = client.get("profile_id")
        if not profile_id or self.clients.get(profile_id) is not client:
            return
        self.clients.pop(profile_id, None)
        self.state.cancel_player(profile_id)
        rematch_notices = []
        for match_id, requests in list(self.rematches.items()):
            if profile_id in requests:
                self.rematches.pop(match_id, None)
                metadata = self.matches.get(match_id, {})
                for other_id in metadata.get("profiles", []):
                    if other_id != profile_id:
                        rematch_notices.append((other_id, match_id))
        notices = []
        for inviter, invite in list(self.state.invites.items()):
            target = invite["target_profile_id"]
            if inviter == profile_id:
                self.state.invites.pop(inviter, None)
                notices.append((target, "server-push", profile_id, "incoming"))
            elif target == profile_id:
                self.state.invites.pop(inviter, None)
                notices.append((inviter, invite["request_id"], profile_id, "outgoing"))
        for other_id, request_id, subject_id, direction in notices:
            other = self.clients.get(other_id)
            if other:
                try:
                    await self.send(other["websocket"], envelope("friend_invite_status", request_id, {"profile_id": subject_id, "direction": direction, "status": "unavailable"}))
                except Exception:
                    pass
        for other_id, match_id in rematch_notices:
            other = self.clients.get(other_id)
            if other:
                try:
                    await self.send(other["websocket"], envelope("rematch_status", "server-push", {"status": "cancelled"}, match_id))
                except Exception:
                    pass

    async def defer_or_error(self, first, second, requeue, code="match_server_unavailable", message="Match server is unavailable"):
        if requeue:
            self.requeue_player(first)
            self.requeue_player(second)
            return
        for player in (first, second):
            client = self.clients.get(player["profile_id"])
            if client:
                try: await self.error(client["websocket"], player["request_id"], code, message)
                except Exception: pass

    async def create_match(self, first, second, rated, requeue=False):
        async with self.lock:
            server = self.state.select_server()
            if not server or not server.websocket:
                await self.defer_or_error(first, second, requeue)
                return False
            control = server.websocket
            match_id, command_id = str(uuid.uuid4()), secrets.token_urlsafe(18)
            future = asyncio.get_running_loop().create_future()
            self.commands[command_id] = (future, server.server_id, match_id)
            players = []
            for slot, player in enumerate((first, second), 1):
                players.append({"player_id": "player_%d" % slot, "slot": slot, "profile_id": player["profile_id"], "name": player.get("player_name", ""), "join_token": secrets.token_urlsafe(32), "rated": rated})
            command = envelope("create_match", command_id, {"match_id": match_id, "players": players, "language": first["language"], "rules_version": first["rules_version"], "rated": rated})
            command["command_id"] = command_id
            try:
                await self.send(control, command)
            except Exception:
                if server.websocket is control:
                    server.websocket = None
                self.commands.pop(command_id, None); self.state.release(server.server_id)
                await self.defer_or_error(first, second, requeue)
                return False
        try:
            result = await asyncio.wait_for(future, COMMAND_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            async with self.lock:
                self.commands.pop(command_id, None); self.state.release(server.server_id)
                if server.websocket is control:
                    server.websocket = None
                await self.defer_or_error(first, second, requeue)
            return False
        async with self.lock:
            self.commands.pop(command_id, None)
            self.state.release(server.server_id)
            self.prune_lifecycle()
            cancelled = [player for player in (first, second) if player["profile_id"] in self.cancelled]
            if cancelled:
                active = [player for player in (first, second) if player not in cancelled]
                if requeue:
                    for player in active:
                        self.requeue_player(player)
                for player in active:
                    client = self.clients.get(player["profile_id"])
                    if client:
                        await self.send(client["websocket"], envelope("matchmaking_status", player["request_id"], {
                            "status": "searching" if requeue else "cancelled",
                            "queue_size": len(self.state.queue.entries),
                        }))
                return False
            if result.get("ok") is not True:
                if result.get("code") == "capacity_exceeded":
                    server.active_matches = server.max_matches
                    await self.defer_or_error(first, second, requeue, "capacity_exceeded", "Match servers have no free capacity")
                else:
                    for player in (first, second):
                        client = self.clients.get(player["profile_id"])
                        if client:
                            try: await self.error(client["websocket"], player["request_id"], result.get("code", "match_create_failed"), result.get("message", "Match could not be created"))
                            except Exception: pass
                return False
            self.state.routes[match_id] = server.server_id
            self.matches[match_id] = {
                "profiles": [first["profile_id"], second["profile_id"]],
                "language": first["language"],
                "rules_version": first["rules_version"],
                "rated": rated,
                "requeue": requeue,
                "seats": {player["profile_id"]: details["player_id"] for player, details in zip((first, second), players)},
            }
        for player, details in zip((first, second), players):
            if any(owner_id in self.cancelled for owner_id in self.matches[match_id]["profiles"]):
                break
            await self.assign(player, envelope("match_assigned", player["request_id"], {"operation": "join", "server_id": server.server_id, "server_url": server.public_url, "player_id": details["player_id"], "join_token": details["join_token"]}, match_id), server.server_id, details["player_id"])
        return match_id

    async def try_pairs(self):
        async with self.lock:
            pairs = self.state.pairs(fallback_seconds=self.fallback_seconds)
        for first, second in pairs:
            await self.create_match(first, second, True, True)

    async def matchmaking_loop(self):
        while True:
            await asyncio.sleep(1)
            await self.try_pairs()

    async def client_message(self, websocket, client, message):
        request_id, kind, payload = message["request_id"], message["type"], message["payload"]
        profile_id = client.get("profile_id")
        if kind == "client_hello":
            token = payload.get("profile_token")
            if token is not None and (not isinstance(token, str) or not 32 <= len(token) <= 128):
                return await self.error(websocket, request_id, "invalid_profile_token", "Invalid profile token")
            try: name = player_name(payload.get("player_name"))
            except ValueError as error: return await self.error(websocket, request_id, "invalid_player_name", str(error))
            try: profile = await self.resolve_profile(token)
            except (urllib.error.URLError, ValueError, json.JSONDecodeError): return await self.error(websocket, request_id, "profile_service_unavailable", "Profile service is unavailable")
            if type(profile.get("profile_id")) is not int or type(profile.get("rating")) is not int: return await self.error(websocket, request_id, "profile_service_invalid", "Profile service returned invalid data")
            previous = self.clients.get(profile["profile_id"])
            if previous is not None and previous is not client:
                previous["profile_id"] = None; previous["superseded"] = True
            client.update(profile); client["player_name"] = name; self.clients[profile["profile_id"]] = client
            await self.send(websocket, envelope("server_hello", request_id, {**profile, "server_time": time.time(), "session_id": client["session_id"]}))
            return await self.resend_assignment(profile["profile_id"])
        if not profile_id: return await self.error(websocket, request_id, "profile_not_ready", "Send client_hello first")
        if kind == "find_match":
            fields = (payload.get("mode", "default"), payload.get("language"), payload.get("rules_version"))
            if not all(isinstance(value, str) and 0 < len(value) <= MAX_TEXT for value in fields): return await self.error(websocket, request_id, "invalid_matchmaking_request", "Invalid matchmaking request")
            tier = payload.get("queue_tier", "regular")
            if tier not in ("regular", "fallback"):
                return await self.error(websocket, request_id, "invalid_queue_tier", "Invalid queue tier")
            if fields[0] not in self.state.queue.modes:
                return await self.error(websocket, request_id, "invalid_matchmaking_request", "Unknown matchmaking mode")
            item = {"ticket_id": secrets.token_urlsafe(12), "profile_id": profile_id, "player_name": client.get("player_name", ""), "rating": client["rating"], "request_id": request_id, "mode": fields[0], "language": fields[1], "rules_version": fields[2], "queued_at": time.time(), "queue_tier": tier, "fallback_seconds": self.fallback_seconds}
            async with self.lock:
                self.prune_lifecycle(); self.cancelled.pop(profile_id, None)
                self.state.queue_player(item)
                size = len(self.state.queue.entries)
            await self.send(websocket, envelope("matchmaking_status", request_id, {
                "status": "searching",
                "queue_size": size,
            }))
            return await self.try_pairs()
        if kind == "cancel_matchmaking":
            return await self.cancel_pair(profile_id, request_id)
        if kind == "join_match":
            match_id, hint = message.get("match_id", ""), payload.get("server_id", "")
            player_id, join_token = payload.get("player_id"), payload.get("join_token")
            if not all(isinstance(value, str) and value for value in (match_id, hint, player_id, join_token)):
                return await self.error(websocket, request_id, "invalid_join", "Invalid pending match assignment")
            async with self.lock: server = self.state.route(match_id, hint)
            if not server: return await self.error(websocket, request_id, "match_server_unavailable", "Match server is unavailable")
            return await self.send(websocket, envelope("match_assigned", request_id, {
                "operation": "join", "server_id": server.server_id, "server_url": server.public_url,
                "player_id": player_id, "join_token": join_token,
            }, match_id))
        if kind == "send_friend_invite":
            target = payload.get("target_profile_id")
            language, rules = payload.get("language"), payload.get("rules_version")
            if type(target) is not int or target < 1 or target == profile_id or not all(isinstance(value, str) and 0 < len(value) <= MAX_TEXT for value in (language, rules)):
                return await self.error(websocket, request_id, "invalid_friend_invite", "Invalid friend invite")
            async with self.lock:
                if profile_id in self.state.invites or any(invite["target_profile_id"] in (profile_id, target) or inviter in (profile_id, target) for inviter, invite in self.state.invites.items()):
                    return await self.error(websocket, request_id, "duplicate_friend_invite", "Friend invite already pending")
                target_client = self.clients.get(target)
                if not target_client:
                    return await self.send(websocket, envelope("friend_invite_status", request_id, {"profile_id": target, "direction": "outgoing", "status": "unavailable"}))
                self.state.invites[profile_id] = {"target_profile_id": target, "language": language, "rules_version": rules, "request_id": request_id}
            await self.send(websocket, envelope("friend_invite_status", request_id, {"profile_id": target, "direction": "outgoing", "status": "pending"}))
            return await self.send(target_client["websocket"], envelope("friend_invite_status", "server-push", {"profile_id": profile_id, "direction": "incoming", "status": "pending"}))
        if kind == "cancel_friend_invite":
            target = payload.get("target_profile_id")
            async with self.lock:
                invite = self.state.invites.get(profile_id)
                if not invite or invite["target_profile_id"] != target: return await self.error(websocket, request_id, "friend_invite_not_found", "Friend invite was not found")
                self.state.invites.pop(profile_id); target_client = self.clients.get(target)
            await self.send(websocket, envelope("friend_invite_status", request_id, {"profile_id": target, "direction": "outgoing", "status": "cancelled"}))
            if target_client: await self.send(target_client["websocket"], envelope("friend_invite_status", "server-push", {"profile_id": profile_id, "direction": "incoming", "status": "cancelled"}))
            return
        if kind == "respond_friend_invite":
            inviter = payload.get("inviter_profile_id")
            accepted, language, rules = payload.get("accepted"), payload.get("language"), payload.get("rules_version")
            async with self.lock:
                invite = self.state.invites.get(inviter)
                if not invite or invite["target_profile_id"] != profile_id: return await self.error(websocket, request_id, "friend_invite_not_found", "Friend invite was not found")
                self.state.invites.pop(inviter); inviter_client = self.clients.get(inviter)
            status = ("declined" if accepted is not True else
                      "unavailable" if not inviter_client else
                      "language_mismatch" if language != invite["language"] else
                      "rules_version_mismatch" if rules != invite["rules_version"] else None)
            if status:
                await self.send(websocket, envelope("friend_invite_status", request_id, {"profile_id": inviter, "direction": "incoming", "status": status}))
                if inviter_client: await self.send(inviter_client["websocket"], envelope("friend_invite_status", invite["request_id"], {"profile_id": profile_id, "direction": "outgoing", "status": status}))
                return
            language, rules = invite["language"], invite["rules_version"]
            for socket, notice in ((websocket, envelope("friend_invite_status", request_id, {"profile_id": inviter, "direction": "incoming", "status": "accepted"})), (inviter_client["websocket"], envelope("friend_invite_status", invite["request_id"], {"profile_id": profile_id, "direction": "outgoing", "status": "accepted"}))):
                try: await self.send(socket, notice)
                except Exception: pass
            first = {"profile_id": inviter, "player_name": inviter_client.get("player_name", ""), "rating": inviter_client["rating"], "request_id": invite["request_id"], "mode": "friend", "language": language, "rules_version": rules, "queued_at": time.time()}
            second = {"profile_id": profile_id, "player_name": client.get("player_name", ""), "rating": client["rating"], "request_id": request_id, "mode": "friend", "language": language, "rules_version": rules, "queued_at": time.time()}
            return await self.create_match(first, second, False)
        if kind == "resume_match":
            match_id, hint = message.get("match_id", ""), payload.get("server_id", "")
            if not isinstance(match_id, str) or not match_id or not isinstance(hint, str): return await self.error(websocket, request_id, "invalid_match_id", "Invalid match id")
            async with self.lock: server = self.state.route(match_id, hint)
            if not server: return await self.error(websocket, request_id, "match_server_unavailable", "Match server is unavailable")
            return await self.send(websocket, envelope("match_assigned", request_id, {"operation": "resume", "server_id": server.server_id, "server_url": server.public_url}, match_id))
        if kind == "request_rematch":
            match_id = message.get("match_id", "")
            metadata = self.matches.get(match_id)
            if not metadata or profile_id not in metadata["profiles"]:
                return await self.error(websocket, request_id, "rematch_unavailable", "Rematch is unavailable")
            if metadata.get("rematched"):
                pending = self.pending_assignments.get(profile_id)
                if pending and pending["message"].get("match_id") == metadata.get("rematch_match_id"):
                    return await self.resend_assignment(profile_id)
                return await self.error(websocket, request_id, "rematch_unavailable", "Rematch is unavailable")
            if metadata.get("rematching"):
                return await self.error(websocket, request_id, "rematch_unavailable", "Rematch is unavailable")
            if match_id not in self.rematches:
                try: completed = await self.proxy("/matches/state/load", {"profile_id": profile_id, "match_id": match_id})
                except (urllib.error.URLError, ValueError, json.JSONDecodeError): return await self.error(websocket, request_id, "data_service_unavailable", "Database service is unavailable")
                if completed.get("found") is True and completed.get("status") != "completed":
                    return await self.error(websocket, request_id, "match_not_completed", "Match is not completed")
                if completed.get("found") is not True:
                    try: completed = await self.proxy("/matches/detail", {"profile_id": profile_id, "match_id": match_id})
                    except (urllib.error.URLError, ValueError, json.JSONDecodeError): return await self.error(websocket, request_id, "data_service_unavailable", "Database service is unavailable")
                    if completed.get("found") is not True:
                        return await self.error(websocket, request_id, "match_not_completed", "Match is not completed")
            requests = self.rematches.setdefault(match_id, {})
            requests[profile_id] = request_id
            opponent_id = next(value for value in metadata["profiles"] if value != profile_id)
            if opponent_id not in requests:
                await self.send(websocket, envelope("rematch_status", request_id, {"status": "waiting"}, match_id))
                opponent = self.clients.get(opponent_id)
                if opponent:
                    await self.send(opponent["websocket"], envelope("rematch_status", "server-push", {"status": "requested"}, match_id))
                return
            players = []
            for owner_id in metadata["profiles"]:
                owner = self.clients.get(owner_id)
                if not owner:
                    return await self.send(websocket, envelope("rematch_status", request_id, {"status": "waiting"}, match_id))
                players.append({
                    "profile_id": owner_id, "player_name": owner.get("player_name", ""), "rating": owner["rating"], "request_id": requests[owner_id],
                    "mode": "rematch", "language": metadata["language"],
                    "rules_version": metadata["rules_version"], "queued_at": time.time(),
                })
            self.rematches.pop(match_id, None)
            metadata["rematching"] = True
            try:
                created = await self.create_match(players[0], players[1], metadata["rated"])
            finally:
                metadata["rematching"] = False
                if metadata.pop("rematch_cancelled", False):
                    for owner_id in metadata["profiles"]:
                        self.cancelled.pop(owner_id, None)
            metadata["rematched"] = bool(created)
            if created:
                metadata["rematch_match_id"] = created
            return created
        if kind == "cancel_rematch":
            match_id = message.get("match_id", "")
            requests = self.rematches.pop(match_id, {})
            metadata = self.matches.get(match_id, {})
            if metadata.get("rematching"):
                metadata["rematch_cancelled"] = True
                for owner_id in metadata.get("profiles", []):
                    self.cancelled[owner_id] = time.monotonic() + COMMAND_TIMEOUT_SECONDS + 1
            await self.send(websocket, envelope("rematch_status", request_id, {"status": "cancelled"}, match_id))
            for other_id in metadata.get("profiles", []):
                if other_id != profile_id:
                    other = self.clients.get(other_id)
                    if other:
                        try: await self.send(other["websocket"], envelope("rematch_status", "server-push", {"status": "cancelled"}, match_id))
                        except Exception: pass
            return
        if kind in ("request_match_history", "request_match_detail"):
            try: result = await self.proxy("/matches/history" if kind.endswith("history") else "/matches/detail", {"profile_id": profile_id, **({"match_id": message.get("match_id", "")} if kind.endswith("detail") else {})})
            except (urllib.error.URLError, ValueError, json.JSONDecodeError): return await self.error(websocket, request_id, "data_service_unavailable", "Database service is unavailable")
            return await self.send(websocket, envelope("match_history" if kind.endswith("history") else "match_detail", request_id, result, message.get("match_id")))
        return await self.error(websocket, request_id, "not_implemented", "Message is not implemented")

    async def server_message(self, websocket, server_id, message):
        kind, payload = message["type"], message["payload"]
        if kind == "server_register":
            if not secrets.compare_digest(str(payload.get("token", "")), self.server_token): return False
            try:
                server = self.state.register(payload.get("server_id", ""), payload.get("public_url", ""), payload.get("max_matches"), payload.get("match_ids", []), payload.get("active_matches"))
            except ValueError: return False
            server.websocket = websocket
            await self.send(websocket, {"v": VERSION, "type": "server_registered", "payload": {"server_id": server.server_id}})
            return server.server_id
        server = self.state.servers.get(server_id)
        if not server or server.websocket is not websocket: return False
        if kind == "server_heartbeat":
            try: self.state.heartbeat(server_id, payload.get("match_ids", []), payload.get("active_matches"), payload.get("max_matches"))
            except ValueError: return False
            return True
        if kind == "create_match_result":
            command_id, command = message.get("command_id"), self.commands.get(message.get("command_id"))
            if command and command[1] == server_id and not command[0].done():
                command[0].set_result(payload)
            return True
        if kind == "match_player_joined":
            match_id, profile_id, player_id = payload.get("match_id"), payload.get("profile_id"), payload.get("player_id")
            pending = self.pending_assignments.get(profile_id)
            metadata = self.matches.get(match_id)
            if (not isinstance(match_id, str) or type(profile_id) is not int or not isinstance(player_id, str)
                    or self.state.routes.get(match_id) != server_id or not metadata
                    or metadata.get("seats", {}).get(profile_id) != player_id
                    or (pending and (pending["match_id"], pending["server_id"], pending["player_id"]) != (match_id, server_id, player_id))):
                return False
            self.pending_assignments.pop(profile_id, None)
            return True
        return False

    async def handler(self, websocket):
        client, server_id = {"websocket": websocket, "session_id": secrets.token_urlsafe(12)}, None
        try:
            async for raw in websocket:
                if not isinstance(raw, str) or len(raw.encode()) > MAX_MESSAGE_BYTES: break
                try: message = json.loads(raw)
                except json.JSONDecodeError: await self.error(websocket, "server-push", "invalid_json", "Invalid JSON"); continue
                if not isinstance(message, dict) or message.get("v") != VERSION or not isinstance(message.get("type"), str) or not isinstance(message.get("payload"), dict):
                    await self.error(websocket, "server-push", "invalid_envelope", "Invalid envelope"); continue
                if message["type"] == "admin_status":
                    if not isinstance(message.get("request_id"), str) or not await self.admin_message(websocket, message): break
                elif message["type"].startswith("server_") or message["type"] in ("create_match_result", "match_player_joined"):
                    result = await self.server_message(websocket, server_id, message)
                    if message["type"] == "server_register" and result: server_id = result
                    if result is False: break
                elif not isinstance(message.get("request_id"), str):
                    await self.error(websocket, "server-push", "invalid_envelope", "Invalid envelope")
                else:
                    await self.client_message(websocket, client, message)
        finally:
            await self.disconnect_client(client)
            if server_id and self.state.servers.get(server_id, None) and self.state.servers[server_id].websocket is websocket: self.state.servers[server_id].websocket = None


async def main():
    try:
        import websockets
    except ImportError as error:
        raise SystemExit("Install the websockets package to run orchestrator.py") from error
    parser = argparse.ArgumentParser(); parser.add_argument("--host", default=os.getenv("ORCHESTRATOR_HOST", "0.0.0.0")); parser.add_argument("--port", type=int, default=int(os.getenv("ORCHESTRATOR_PORT", "9000")))
    args = parser.parse_args()
    token = os.getenv("ORCHESTRATOR_SERVER_TOKEN", "")
    if not token: raise SystemExit("ORCHESTRATOR_SERVER_TOKEN is required")
    fallback_seconds = float(os.getenv("FALLBACK_SECONDS", str(DEFAULT_FALLBACK_SECONDS)))
    if fallback_seconds < 0: raise SystemExit("FALLBACK_SECONDS must not be negative")
    db_token = os.getenv("DB_SERVICE_TOKEN", "")
    if not db_token: raise SystemExit("DB_SERVICE_TOKEN is required")
    app = Orchestrator(os.getenv("DB_SERVICE_URL", "http://127.0.0.1:8080"), token, fallback_seconds, db_token)
    api_port = int(os.getenv("ORCHESTRATOR_API_PORT", "9001"))
    api = await serve_queue_api(app, args.host, api_port)
    async with api, websockets.serve(app.handler, args.host, args.port, max_size=MAX_MESSAGE_BYTES):
        await app.matchmaking_loop()


if __name__ == "__main__":
    asyncio.run(main())
