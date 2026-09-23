import asyncio
import time
import json
import unittest
from unittest.mock import AsyncMock, patch

from orchestrator.modes import enabled
from service import Orchestrator, PENDING_ASSIGNMENT_SECONDS, STALE_SERVER_SECONDS, State, VERSION, envelope, rating_gap


def queued(profile_id, rating, when=0, **extra):
    return {"profile_id": profile_id, "rating": rating, "request_id": "r", "mode": "default", "language": "ru", "rules_version": "1", "queued_at": when, **extra}


class StateTests(unittest.TestCase):
    def test_modes_can_be_disabled(self):
        with patch.dict("os.environ", {"ORCHESTRATOR_MODES": ""}):
            self.assertEqual(enabled(), {})

    def test_rating_gap_and_closest_pair(self):
        self.assertEqual((rating_gap(0), rating_gap(15), rating_gap(9999)), (100, 150, 400))
        state = State()
        state.queue_player(queued(1, 1200)); state.queue_player(queued(2, 1280, 1)); state.queue_player(queued(3, 1210, 2))
        self.assertEqual([(a["profile_id"], b["profile_id"]) for a, b in state.pairs(2)], [(1, 3)])

    def test_fallback_is_only_used_after_regular_priority(self):
        state = State()
        state.queue_player(queued(1, 1200, 1))
        state.queue_player({**queued(90, 1200, 0), "queue_tier": "fallback"})
        self.assertEqual(state.pairs(4, fallback_seconds=5), [])
        state.queue_player(queued(2, 1800, 2))
        self.assertEqual([(a["profile_id"], b["profile_id"]) for a, b in state.pairs(10, 5)], [(1, 2)])
        state.queue_player(queued(3, 2000, 3))
        self.assertEqual([(a["profile_id"], b["profile_id"]) for a, b in state.pairs(10, 5)], [(3, 90)])

    def test_estimate_waits_for_a_queued_opponent_rating_gap(self):
        state = State()
        state.queue_player(queued(1, 1200, 0, fallback_seconds=60)); state.queue_player(queued(2, 1340, 4, fallback_seconds=60))
        # 140 points need the 150 gap, which the older ticket reaches at 15s: the newer one waits 11s
        self.assertEqual(state.queue.estimate(state.queue.entries[1], 5), 11)
        state.queue_player(queued(3, 1250, 5, fallback_seconds=60))
        self.assertEqual(state.queue.estimate(state.queue.entries[2], 5), 0)

    def test_estimate_uses_live_bot_pool_and_forgets_a_missing_bot(self):
        state = State()
        state.queue_player(queued(1, 1200, 0, fallback_seconds=10))
        ticket = state.queue.entries[0]
        self.assertIsNone(state.queue.estimate(ticket, 1))
        state.queue.stats.note_bot_poll(0); state.queue.stats.note_bot_poll(2)
        self.assertEqual(state.queue.estimate(ticket, 3), 10 + 2 + 2)
        self.assertIsNone(state.queue.estimate(ticket, 2 + 15.5))

    def test_estimate_learns_human_waits_and_bot_delays(self):
        state = State()
        state.queue_player(queued(1, 1200, 0, fallback_seconds=60)); state.queue_player(queued(2, 1200, 20, fallback_seconds=60))
        state.pairs(20, 60)
        state.queue_player(queued(3, 1200, 100, fallback_seconds=60))
        self.assertEqual(state.queue.estimate(state.queue.entries[0], 101), 10)
        self.assertIsNone(state.queue.estimate(state.queue.entries[0], 111))
        state.queue.stats.note_bot_poll(160)
        state.queue_player({**queued(90, 1200, 161), "queue_tier": "fallback"})
        state.pairs(165, 60)
        state.queue_player(queued(4, 1200, 200, fallback_seconds=60))
        state.queue.stats.note_bot_poll(211)
        # past the 10s human median, only the bot's learned 5s delay predicts the wait
        self.assertEqual(state.queue.estimate(state.queue.entries[0], 211), 60 + 5)

    def test_least_loaded_reservation_and_capacity(self):
        state = State(); state.register("a", "ws://a", 2); state.register("b", "ws://b", 2)
        state.servers["a"].websocket = state.servers["b"].websocket = object()
        state.servers["a"].active_matches = 1
        self.assertEqual(state.select_server().server_id, "b")
        self.assertEqual(state.select_server().server_id, "a")
        self.assertEqual(state.select_server().server_id, "b")
        self.assertIsNone(state.select_server())

    def test_disconnected_server_is_not_selected(self):
        state = State(); state.register("a", "ws://a", 2)
        self.assertIsNone(state.select_server())

    def test_heartbeat_rebuilds_route_and_stale_is_not_routable(self):
        state = State(); state.register("a", "ws://a", 5, ["match-1"])
        self.assertEqual(state.route("match-1").server_id, "a")
        state.servers["a"].seen_at = time.monotonic() - STALE_SERVER_SECONDS - 1
        self.assertIsNone(state.route("match-1"))

    def test_invalid_heartbeat_is_rejected(self):
        state = State(); state.register("a", "ws://a", 5)
        with self.assertRaises(ValueError): state.heartbeat("a", ["ok", 3], 2)
        with self.assertRaises(ValueError): state.register("b", "ws://b", None)


class FakeWebSocket:
    def __init__(self): self.sent = []
    async def send(self, value): self.sent.append(json.loads(value))


class FailingWebSocket(FakeWebSocket):
    async def send(self, value): raise ConnectionError("closed")


class FakeConnection(FakeWebSocket):
    """Feeds a fixed message list into handler()'s `async for raw in websocket`."""
    def __init__(self, messages): super().__init__(); self._messages = messages

    async def __aiter__(self):
        for message in self._messages:
            yield json.dumps(message)


class RoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_queue_api_is_anonymized(self):
        app = Orchestrator("http://unused", "secret", fallback_seconds=5)
        app.state.queue_player({**queued(1, 1200, time.time() - 6), "queue_tier": "regular", "fallback_seconds": 5})
        status, body = await app.api_response("GET", "/v1/matchmaking/queue", {})
        self.assertEqual(status, "200 OK")
        self.assertEqual(set(body["entries"][0]), {"ticket_id", "mode", "language", "rules_version", "rating", "wait_seconds", "fallback_eligible"})
        self.assertNotEqual(body["entries"][0]["ticket_id"], "1")
        self.assertTrue(body["entries"][0]["fallback_eligible"])
        self.assertEqual((await app.api_response("POST", "/v1/matchmaking/queue", {}))[0], "404 Not Found")

    async def test_matchmaking_status_has_no_estimate_without_bots_or_history(self):
        app, socket = Orchestrator("http://unused", "secret", fallback_seconds=7), FakeWebSocket()
        client = {"profile_id": 1, "rating": 1200, "websocket": socket}
        app.clients[1] = client
        app.try_pairs = AsyncMock()

        with patch("service.time.time", return_value=100):
            await app.client_message(socket, client, {
                "v": VERSION, "type": "find_match", "request_id": "find", "payload": {
                    "mode": "default", "language": "ru", "rules_version": "1",
                },
            })

        self.assertEqual(socket.sent[-1]["payload"], {
            "status": "searching", "queue_size": 1, "wait_seconds": 0.0, "estimated_wait_seconds": None,
        })

    async def test_estimate_is_pushed_when_bot_pool_appears(self):
        app, socket = Orchestrator("http://unused", "secret", fallback_seconds=7), FakeWebSocket()
        app.clients[1] = {"profile_id": 1, "rating": 1200, "websocket": socket}
        app.state.queue_player({**queued(1, 1200, 100), "queue_tier": "regular", "fallback_seconds": 7, "reported_estimate": None})

        with patch("service.time.time", return_value=102):
            await app.publish_estimates()
            self.assertEqual(socket.sent, [])
            await app.api_response("GET", "/v1/matchmaking/queue", {})
            await app.publish_estimates()
            await app.publish_estimates()

        self.assertEqual(len(socket.sent), 1)
        self.assertEqual(socket.sent[0]["request_id"], "r")
        self.assertEqual(socket.sent[0]["payload"]["wait_seconds"], 2.0)
        self.assertEqual(socket.sent[0]["payload"]["estimated_wait_seconds"], 9)

    async def test_fallback_tier_creates_rated_pair(self):
        app, socket = Orchestrator("http://unused", "secret", 0), FakeWebSocket()
        app.resolve_profile = AsyncMock(return_value={"profile_id": 90, "rating": 1200, "profile_token": "x" * 32})
        client = {"websocket": socket, "session_id": "bot"}
        await app.client_message(socket, client, {
            "v": VERSION, "type": "client_hello", "request_id": "hello", "payload": {},
        })
        app.state.queue_player(queued(1, 1200, time.time() - 10))
        app.create_match = AsyncMock(return_value=True)
        await app.client_message(socket, client, {
            "v": VERSION, "type": "find_match", "request_id": "find", "payload": {
                "mode": "default", "language": "ru", "rules_version": "1", "queue_tier": "fallback",
            },
        })
        first, second, rated = app.create_match.await_args.args[:3]
        self.assertTrue(rated)
        self.assertEqual({first["queue_tier"], second["queue_tier"]}, {"regular", "fallback"})
        self.assertEqual(app.state.queue, [])

    async def test_failed_fallback_match_requeues_both_tiers(self):
        app = Orchestrator("http://unused", "secret")
        fallback = {**queued(90, 1200), "queue_tier": "fallback"}

        await app.defer_or_error(queued(1, 1200), fallback, True)

        self.assertEqual([item["profile_id"] for item in app.state.queue], [1, 90])

    async def test_admin_status_is_authenticated_and_reports_server_load(self):
        app, admin = Orchestrator("http://unused", "secret"), FakeWebSocket()
        server = app.state.register("a", "ws://a", 5, ["match-1"], 1)
        server.websocket = object()
        server.reservations = 1
        app.clients[7] = {}
        app.state.queue.append(queued(8, 1200))

        denied = await app.admin_message(admin, {
            "v": VERSION, "type": "admin_status", "request_id": "status", "payload": {"token": "wrong"},
        })
        self.assertFalse(denied)
        self.assertEqual(admin.sent, [])

        allowed = await app.admin_message(admin, {
            "v": VERSION, "type": "admin_status", "request_id": "status", "payload": {"token": "secret"},
        })
        self.assertTrue(allowed)
        self.assertEqual(admin.sent[-1]["payload"]["clients"], 1)
        self.assertEqual(admin.sent[-1]["payload"]["queue_size"], 1)
        self.assertEqual(admin.sent[-1]["payload"]["servers"][0]["load_percent"], 40.0)

    async def test_disconnect_clears_incoming_invite_and_notifies_inviter(self):
        app = Orchestrator("http://unused", "secret")
        first_socket, second_socket = FakeWebSocket(), FakeWebSocket()
        first = {"profile_id": 1, "websocket": first_socket}
        second = {"profile_id": 2, "websocket": second_socket}
        app.clients = {1: first, 2: second}
        app.state.invites[1] = {"target_profile_id": 2, "request_id": "invite-1"}

        await app.disconnect_client(second)

        self.assertEqual(app.state.invites, {})
        self.assertEqual(first_socket.sent[-1]["payload"], {
            "profile_id": 2, "direction": "outgoing", "status": "unavailable",
        })

    async def test_accepted_friend_invite_notifies_both_and_creates_match(self):
        app = Orchestrator("http://unused", "secret")
        inviter_socket, invitee_socket = FakeWebSocket(), FakeWebSocket()
        inviter = {"profile_id": 1, "player_name": "Alice", "rating": 1200, "websocket": inviter_socket}
        invitee = {"profile_id": 2, "player_name": "Bob", "rating": 1201, "websocket": invitee_socket}
        app.clients = {1: inviter, 2: invitee}
        app.state.invites[1] = {"target_profile_id": 2, "language": "ru", "rules_version": "1", "request_id": "invite"}
        app.create_match = AsyncMock(return_value=True)

        await app.client_message(invitee_socket, invitee, {"v": VERSION, "type": "respond_friend_invite", "request_id": "response", "payload": {"inviter_profile_id": 1, "accepted": True, "language": "ru", "rules_version": "1"}})

        app.create_match.assert_awaited_once()
        first, second, rated = app.create_match.await_args.args
        self.assertEqual((first["profile_id"], first["player_name"], first["rating"], first["request_id"]), (1, "Alice", 1200, "invite"))
        self.assertEqual((second["profile_id"], second["player_name"], second["rating"], second["request_id"]), (2, "Bob", 1201, "response"))
        self.assertEqual((first["language"], first["rules_version"]), ("ru", "1"))
        self.assertEqual((second["language"], second["rules_version"]), ("ru", "1"))
        self.assertFalse(rated)
        self.assertEqual(inviter_socket.sent[-1]["payload"]["status"], "accepted")
        self.assertEqual(invitee_socket.sent[-1]["payload"]["status"], "accepted")

    async def test_friend_invite_reports_each_match_contract_mismatch(self):
        for language, rules, expected in (("en", "1", "language_mismatch"),
                                          ("ru", "other", "rules_version_mismatch")):
            with self.subTest(expected):
                app = Orchestrator("http://unused", "secret")
                inviter_socket, invitee_socket = FakeWebSocket(), FakeWebSocket()
                inviter = {"profile_id": 1, "player_name": "Alice", "rating": 1200, "websocket": inviter_socket}
                invitee = {"profile_id": 2, "player_name": "Bob", "rating": 1201, "websocket": invitee_socket}
                app.clients = {1: inviter, 2: invitee}
                app.state.invites[1] = {"target_profile_id": 2, "language": "ru", "rules_version": "1", "request_id": "invite"}
                app.create_match = AsyncMock()

                await app.client_message(invitee_socket, invitee, {"v": VERSION, "type": "respond_friend_invite", "request_id": "response", "payload": {"inviter_profile_id": 1, "accepted": True, "language": language, "rules_version": rules}})

                self.assertEqual(inviter_socket.sent[-1]["payload"]["status"], expected)
                self.assertEqual(invitee_socket.sent[-1]["payload"]["status"], expected)
                app.create_match.assert_not_awaited()

    async def test_accepted_friend_invite_creates_match_when_inviter_status_fails(self):
        app = Orchestrator("http://unused", "secret")
        inviter = {"profile_id": 1, "player_name": "Alice", "rating": 1200, "websocket": FailingWebSocket()}
        invitee_socket = FakeWebSocket(); invitee = {"profile_id": 2, "player_name": "Bob", "rating": 1201, "websocket": invitee_socket}
        app.clients = {1: inviter, 2: invitee}
        app.state.invites[1] = {"target_profile_id": 2, "language": "ru", "rules_version": "1", "request_id": "invite"}

        self.assertFalse(await app.client_message(invitee_socket, invitee, {"v": VERSION, "type": "respond_friend_invite", "request_id": "response", "payload": {"inviter_profile_id": 1, "accepted": True, "language": "ru", "rules_version": "1"}}))

        self.assertEqual(invitee_socket.sent[-1]["payload"]["code"], "match_server_unavailable")

    async def test_create_result_failure_notifies_second_player_after_first_socket_fails(self):
        app, control = Orchestrator("http://unused", "secret"), FakeWebSocket()
        app.state.register("a", "ws://a", 5).websocket = control
        app.clients = {1: {"profile_id": 1, "websocket": FailingWebSocket()}, 2: {"profile_id": 2, "websocket": FakeWebSocket()}}
        creating = asyncio.create_task(app.create_match(queued(1, 1200), queued(2, 1201), False))
        await asyncio.sleep(0); command = control.sent[-1]
        await app.server_message(control, "a", {"v": VERSION, "type": "create_match_result", "command_id": command["command_id"], "payload": {"ok": False, "code": "match_create_failed"}})

        self.assertFalse(await creating)
        self.assertEqual(app.clients[2]["websocket"].sent[-1]["payload"]["code"], "match_create_failed")

    async def test_control_registration_and_pending_join_route(self):
        app, control = Orchestrator("http://unused", "secret"), FakeWebSocket()
        server_id = await app.server_message(control, None, {
            "v": VERSION, "type": "server_register", "payload": {
                "token": "secret", "server_id": "a", "public_url": "ws://a",
                "max_matches": 5, "active_matches": 0, "match_ids": ["match-1"],
            },
        })
        self.assertEqual(server_id, "a")
        client, session = FakeWebSocket(), {"profile_id": 7, "rating": 1200}
        await app.client_message(client, session, {
            "v": VERSION, "type": "join_match", "request_id": "join-1", "match_id": "match-1",
            "payload": {"server_id": "a", "player_id": "player_1", "join_token": "token"},
        })
        self.assertEqual(client.sent[-1]["type"], "match_assigned")
        self.assertEqual(client.sent[-1]["payload"]["server_url"], "ws://a")

    async def test_create_command_ack_assigns_both_players(self):
        app, control = Orchestrator("http://unused", "secret"), FakeWebSocket()
        server = app.state.register("a", "ws://a", 5)
        server.websocket = control
        first_socket, second_socket = FakeWebSocket(), FakeWebSocket()
        app.clients = {
            1: {"profile_id": 1, "websocket": first_socket},
            2: {"profile_id": 2, "websocket": second_socket},
        }

        creating = asyncio.create_task(app.create_match(
            queued(1, 1200, player_name="Alice"), {**queued(2, 1201, player_name="Bob"), "queue_tier": "fallback"}, True,
        ))
        await asyncio.sleep(0)
        command = control.sent[-1]
        self.assertEqual(command["type"], "create_match")
        self.assertEqual([player["name"] for player in command["payload"]["players"]], ["Alice", "Bob"])
        self.assertTrue(all("queue_tier" not in player for player in command["payload"]["players"]))
        await app.server_message(control, "a", {
            "v": VERSION, "type": "create_match_result", "command_id": command["command_id"],
            "payload": {"ok": True},
        })

        self.assertTrue(await creating)
        self.assertEqual(first_socket.sent[-1]["type"], "match_assigned")
        self.assertEqual(second_socket.sent[-1]["type"], "match_assigned")
        self.assertEqual(set(app.pending_assignments), {1, 2})
        self.assertEqual(app.state.route(command["payload"]["match_id"]).server_id, "a")
        joined = {"v": VERSION, "type": "match_player_joined", "payload": {"match_id": command["payload"]["match_id"], "profile_id": 1, "player_id": "player_1"}}
        other_control = FakeWebSocket(); app.state.register("b", "ws://b", 5).websocket = other_control
        self.assertFalse(await app.server_message(other_control, "b", joined))
        self.assertFalse(await app.server_message(control, "a", {**joined, "payload": {**joined["payload"], "player_id": "forged"}}))
        self.assertIn(1, app.pending_assignments)
        self.assertTrue(await app.server_message(control, "a", joined))
        self.assertNotIn(1, app.pending_assignments)

    async def test_direct_pair_is_not_mixed_into_matchmaking_when_no_server_exists(self):
        app = Orchestrator("http://unused", "secret")
        first_socket, second_socket = FakeWebSocket(), FakeWebSocket()
        app.clients = {
            1: {"profile_id": 1, "websocket": first_socket},
            2: {"profile_id": 2, "websocket": second_socket},
        }

        self.assertFalse(await app.create_match(queued(1, 1200), queued(2, 1201), False))

        self.assertEqual(app.state.queue, [])
        self.assertEqual(first_socket.sent[-1]["payload"]["code"], "match_server_unavailable")
        self.assertEqual(second_socket.sent[-1]["payload"]["code"], "match_server_unavailable")

    async def test_two_rematch_consents_create_a_new_match(self):
        app = Orchestrator("http://unused", "secret")
        first_socket, second_socket = FakeWebSocket(), FakeWebSocket()
        app.clients = {
            1: {"profile_id": 1, "player_name": "Alice", "rating": 1201, "websocket": first_socket},
            2: {"profile_id": 2, "player_name": "Bob", "rating": 1202, "websocket": second_socket},
        }
        app.matches["old"] = {"profiles": [1, 2], "language": "ru", "rules_version": "1", "rated": True}
        app.proxy = AsyncMock(return_value={"found": True, "status": "completed"})
        app.create_match = AsyncMock(return_value=True)
        base = {"v": VERSION, "type": "request_rematch", "match_id": "old", "payload": {}}
        await app.client_message(first_socket, app.clients[1], {**base, "request_id": "r1"})
        await app.client_message(second_socket, app.clients[2], {**base, "request_id": "r2"})
        app.create_match.assert_awaited_once()
        self.assertEqual(app.create_match.await_args.args[2], True)
        self.assertEqual([item["player_name"] for item in app.create_match.await_args.args[:2]], ["Alice", "Bob"])
        self.assertTrue(app.matches["old"]["rematched"])

    async def test_cancel_rematch_while_create_is_pending_blocks_late_assignment(self):
        app, control = Orchestrator("http://unused", "secret"), FakeWebSocket()
        app.state.register("a", "ws://a", 5).websocket = control
        one, two = FakeWebSocket(), FakeWebSocket()
        app.clients = {1: {"profile_id": 1, "rating": 1201, "websocket": one}, 2: {"profile_id": 2, "rating": 1202, "websocket": two}}
        app.matches["old"] = {"profiles": [1, 2], "language": "ru", "rules_version": "1", "rated": True}
        app.proxy = AsyncMock(return_value={"found": True, "status": "completed"})
        base = {"v": VERSION, "type": "request_rematch", "match_id": "old", "payload": {}}
        await app.client_message(one, app.clients[1], {**base, "request_id": "r1"})
        creating = asyncio.create_task(app.client_message(two, app.clients[2], {**base, "request_id": "r2"}))
        await asyncio.sleep(0); command = control.sent[-1]
        await app.client_message(one, app.clients[1], {"v": VERSION, "type": "cancel_rematch", "request_id": "cancel", "match_id": "old", "payload": {}})
        await app.server_message(control, "a", {"v": VERSION, "type": "create_match_result", "command_id": command["command_id"], "payload": {"ok": True}})
        self.assertFalse(await creating)
        self.assertFalse(app.matches["old"].get("rematching"))
        self.assertFalse(any(message["type"] == "match_assigned" for message in one.sent + two.sent))
        self.assertNotIn(1, app.cancelled); self.assertNotIn(2, app.cancelled)

    async def test_assignment_failure_isolated_and_resends_on_hello(self):
        app, control = Orchestrator("http://unused", "secret"), FakeWebSocket()
        app.state.register("a", "ws://a", 5).websocket = control
        first, failed = FakeWebSocket(), FailingWebSocket()
        app.clients = {1: {"profile_id": 1, "websocket": first}, 2: {"profile_id": 2, "websocket": failed}}
        creating = asyncio.create_task(app.create_match(queued(1, 1200), queued(2, 1201), True))
        await asyncio.sleep(0); command = control.sent[-1]
        await app.server_message(control, "a", {"v": VERSION, "type": "create_match_result", "command_id": command["command_id"], "payload": {"ok": True}})
        await creating
        self.assertEqual(first.sent[-1]["type"], "match_assigned")
        self.assertIn(2, app.pending_assignments)
        fresh = FakeWebSocket(); fresh_client = {"websocket": fresh, "session_id": "new"}
        app.resolve_profile = AsyncMock(return_value={"profile_id": 2, "rating": 1201})
        await app.client_message(fresh, fresh_client, {"v": VERSION, "type": "client_hello", "request_id": "hello", "payload": {"player_name": "  Bob\n Smith  "}})
        self.assertEqual(fresh_client["player_name"], "Bob Smith")
        self.assertEqual(fresh.sent[-1]["type"], "match_assigned")
        self.assertIn(2, app.pending_assignments)
        pending = app.pending_assignments[2]
        self.assertTrue(await app.server_message(control, "a", {"v": VERSION, "type": "match_player_joined", "payload": {"match_id": pending["match_id"], "profile_id": 2, "player_id": pending["player_id"]}}))
        self.assertNotIn(2, app.pending_assignments)

    async def test_assignment_resends_until_waiting_match_retention_expires(self):
        app, socket = Orchestrator("http://unused", "secret"), FakeWebSocket()
        app.clients[1] = {"profile_id": 1, "websocket": socket}
        message = envelope("match_assigned", "r", {"operation": "join"}, "new")
        with patch("service.time.monotonic", return_value=100):
            await app.assign(queued(1, 1200), message, "a", "player_1")
        self.assertEqual(PENDING_ASSIGNMENT_SECONDS, 300)
        with patch("service.time.monotonic", return_value=399.999):
            self.assertTrue(await app.resend_assignment(1))
        self.assertIn(1, app.pending_assignments)
        with patch("service.time.monotonic", return_value=400):
            self.assertFalse(await app.resend_assignment(1))
        self.assertNotIn(1, app.pending_assignments)

    async def test_cancel_during_creation_prevents_assignment_and_requeues_opponent(self):
        app, control = Orchestrator("http://unused", "secret"), FakeWebSocket()
        app.state.register("a", "ws://a", 5).websocket = control
        one, two = FakeWebSocket(), FakeWebSocket()
        app.clients = {1: {"profile_id": 1, "websocket": one}, 2: {"profile_id": 2, "websocket": two}}
        creating = asyncio.create_task(app.create_match(queued(1, 1200), queued(2, 1201), True, True))
        await asyncio.sleep(0); command = control.sent[-1]
        await app.client_message(one, app.clients[1], {"v": VERSION, "type": "cancel_matchmaking", "request_id": "cancel", "payload": {}})
        await app.server_message(control, "a", {"v": VERSION, "type": "create_match_result", "command_id": command["command_id"], "payload": {"ok": True}})
        self.assertFalse(await creating)
        self.assertFalse(any(message["type"] == "match_assigned" for message in one.sent + two.sent))
        self.assertEqual([item["profile_id"] for item in app.state.queue], [2])

    async def test_cancel_after_assignment_clears_pending_assignment_and_reports_cancelled(self):
        app = Orchestrator("http://unused", "secret")
        control, one, two = FakeWebSocket(), FakeWebSocket(), FakeWebSocket()
        app.state.register("a", "ws://a", 5).websocket = control
        app.clients = {1: {"profile_id": 1, "rating": 1200, "websocket": one}, 2: {"profile_id": 2, "rating": 1201, "websocket": two}}
        app.matches["new"] = {"profiles": [1, 2], "language": "ru", "rules_version": "1", "requeue": True, "seats": {1: "player_1", 2: "player_2"}}
        app.state.routes["new"] = "a"
        await app.assign(queued(1, 1200), envelope("match_assigned", "r", {"operation": "join"}, "new"), "a", "player_1")
        await app.client_message(one, app.clients[1], {"v": VERSION, "type": "cancel_matchmaking", "request_id": "cancel", "payload": {}})
        self.assertNotIn(1, app.pending_assignments)
        self.assertEqual(app.state.queue, [])
        self.assertEqual(one.sent[-1]["payload"], {"status": "cancelled", "queue_size": 0})
        # The match server can still legitimately ack the seat after cancellation (route/seat still match);
        # this is the harmless-consequence case that a separate match-server-side fix (stuck WAITING match
        # force-stop) is responsible for cleaning up. Cancel only stops the orchestrator from re-pushing it.
        self.assertTrue(await app.server_message(control, "a", {"v": VERSION, "type": "match_player_joined", "payload": {"match_id": "new", "profile_id": 1, "player_id": "player_1"}}))
        self.assertNotIn(1, app.pending_assignments)

    async def test_lobby_disconnect_keeps_assignment_until_valid_match_server_ack(self):
        app, control = Orchestrator("http://unused", "secret"), FakeWebSocket()
        app.state.register("a", "ws://a", 5).websocket = control
        lobby = FakeWebSocket(); app.clients[1] = {"profile_id": 1, "rating": 1200, "websocket": lobby}
        app.matches["new"] = {"profiles": [1, 2], "seats": {1: "player_1"}}; app.state.routes["new"] = "a"
        await app.assign(queued(1, 1200), envelope("match_assigned", "r", {"operation": "join"}, "new"), "a", "player_1")
        await app.disconnect_client(app.clients[1])
        self.assertIn(1, app.pending_assignments)
        self.assertTrue(await app.server_message(control, "a", {"v": VERSION, "type": "match_player_joined", "payload": {"match_id": "new", "profile_id": 1, "player_id": "player_1"}}))
        self.assertNotIn(1, app.pending_assignments)

    async def test_cancel_and_disconnect_clear_rematch_for_both(self):
        app = Orchestrator("http://unused", "secret")
        one, two = FakeWebSocket(), FakeWebSocket()
        app.clients = {1: {"profile_id": 1, "websocket": one}, 2: {"profile_id": 2, "websocket": two}}
        app.matches["old"] = {"profiles": [1, 2]}; app.rematches["old"] = {1: "r1", 2: "r2"}
        await app.client_message(one, app.clients[1], {"v": VERSION, "type": "cancel_rematch", "request_id": "cancel", "match_id": "old", "payload": {}})
        self.assertEqual(app.rematches, {}); self.assertEqual(two.sent[-1]["payload"]["status"], "cancelled")
        app.rematches["old"] = {1: "r1", 2: "r2"}
        await app.disconnect_client(app.clients[1])
        self.assertEqual(app.rematches, {}); self.assertEqual(two.sent[-1]["payload"]["status"], "cancelled")

    async def test_rematch_uses_completed_snapshot_and_retries_existing_assignment(self):
        app = Orchestrator("http://unused", "secret")
        one = FakeWebSocket(); app.clients[1] = {"profile_id": 1, "rating": 1200, "websocket": one}
        app.matches["old"] = {"profiles": [1, 2], "language": "ru", "rules_version": "1", "rated": True}
        app.proxy = AsyncMock(return_value={"found": True, "status": "completed"})
        await app.client_message(one, app.clients[1], {"v": VERSION, "type": "request_rematch", "request_id": "r", "match_id": "old", "payload": {}})
        self.assertEqual(app.proxy.await_args.args[0], "/matches/state/load")
        app.matches["old"].update(rematched=True, rematch_match_id="new")
        app.pending_assignments[1] = {"message": {"type": "match_assigned", "match_id": "new"}, "expires_at": time.monotonic() + 10, "source_match_id": None}
        await app.client_message(one, app.clients[1], {"v": VERSION, "type": "request_rematch", "request_id": "retry", "match_id": "old", "payload": {}})
        self.assertEqual(one.sent[-1]["type"], "match_assigned")

    async def test_unrecognised_match_player_joined_keeps_control_link_open(self):
        app = Orchestrator("http://unused", "secret")
        connection = FakeConnection([
            {"v": VERSION, "type": "server_register", "payload": {"token": "secret", "server_id": "a", "public_url": "ws://a", "max_matches": 5}},
            # No matching entry in app.matches: this is the "orchestrator restarted" / mismatched-report case.
            {"v": VERSION, "type": "match_player_joined", "payload": {"match_id": "unknown-match", "profile_id": 1, "player_id": "player_1"}},
            # Proves the loop kept reading after the rejected report above: admin_status still gets answered.
            {"v": VERSION, "type": "admin_status", "request_id": "status", "payload": {"token": "secret"}},
        ])

        await app.handler(connection)

        self.assertEqual([message["type"] for message in connection.sent], ["server_registered", "admin_status"])

    async def test_bad_server_heartbeat_still_closes_control_link(self):
        app = Orchestrator("http://unused", "secret")
        connection = FakeConnection([
            {"v": VERSION, "type": "server_register", "payload": {"token": "secret", "server_id": "a", "public_url": "ws://a", "max_matches": 5}},
            # Invalid match_ids (non-string element) makes state.heartbeat raise, so server_message returns False.
            {"v": VERSION, "type": "server_heartbeat", "payload": {"match_ids": ["ok", 3], "active_matches": 2}},
            # Should never be reached: the bad heartbeat is a genuine protocol failure and must break the loop.
            {"v": VERSION, "type": "admin_status", "request_id": "status", "payload": {"token": "secret"}},
        ])

        await app.handler(connection)

        self.assertEqual([message["type"] for message in connection.sent], ["server_registered"])

    async def test_abrupt_disconnect_is_not_an_error_and_cleans_up(self):
        from service import ConnectionClosed

        class DroppedConnection(FakeConnection):
            async def __aiter__(self):
                async for raw in super().__aiter__(): yield raw
                raise ConnectionClosed("no close frame received or sent")

        app = Orchestrator("http://unused", "secret")
        connection = DroppedConnection([
            {"v": VERSION, "type": "server_register", "payload": {"token": "secret", "server_id": "a", "public_url": "ws://a", "max_matches": 5}},
        ])

        await app.handler(connection)

        self.assertIsNone(app.state.servers["a"].websocket)


if __name__ == "__main__":
    unittest.main()
