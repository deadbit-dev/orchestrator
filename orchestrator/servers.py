"""Match-server records and routing live separately from matchmaking."""
from dataclasses import dataclass, field
import time


@dataclass
class Server:
    server_id: str
    public_url: str
    max_matches: int
    websocket: object = None
    active_matches: int = 0
    reservations: int = 0
    match_ids: set = field(default_factory=set)
    seen_at: float = field(default_factory=time.monotonic)


class ServerRegistry:
    def __init__(self, max_text=128, stale_seconds=20):
        self.servers = {}
        self.routes = {}
        self.max_text = max_text
        self.stale_seconds = stale_seconds

    def register(self, server_id, public_url, max_matches, match_ids=None, active_matches=None):
        if (not isinstance(server_id, str) or not 0 < len(server_id) <= self.max_text
                or not isinstance(public_url, str) or not public_url
                or type(max_matches) is not int or not 1 <= max_matches <= 10000):
            raise ValueError("invalid server registration")
        match_ids = [] if match_ids is None else match_ids
        server = self.servers.get(server_id) or Server(server_id, public_url, max_matches)
        server.public_url, server.max_matches, server.seen_at = public_url, max_matches, time.monotonic()
        self.servers[server_id] = server
        self.heartbeat(server_id, match_ids, len(match_ids) if active_matches is None else active_matches, max_matches)
        return server

    def heartbeat(self, server_id, match_ids, active_matches, max_matches=None):
        server = self.servers.get(server_id)
        if not server or not isinstance(match_ids, list) or len(match_ids) > 10000:
            raise ValueError("invalid heartbeat")
        clean = {match_id for match_id in match_ids if isinstance(match_id, str) and 0 < len(match_id) <= self.max_text}
        if len(clean) != len(match_ids) or type(active_matches) is not int or active_matches < 0:
            raise ValueError("invalid heartbeat")
        if max_matches is not None:
            if type(max_matches) is not int or not 1 <= max_matches <= 10000:
                raise ValueError("invalid max_matches")
            server.max_matches = max_matches
        for match_id in server.match_ids - clean:
            if self.routes.get(match_id) == server_id:
                self.routes.pop(match_id, None)
        server.match_ids, server.active_matches, server.seen_at = clean, active_matches, time.monotonic()
        for match_id in clean:
            self.routes[match_id] = server_id

    def select_server(self, now=None):
        now = time.monotonic() if now is None else now
        choices = [server for server in self.servers.values()
                   if server.websocket is not None and now - server.seen_at <= self.stale_seconds
                   and server.active_matches + server.reservations < server.max_matches]
        if not choices:
            return None
        server = min(choices, key=lambda item: (item.active_matches + item.reservations, item.server_id))
        server.reservations += 1
        return server

    def release(self, server_id):
        server = self.servers.get(server_id)
        if server:
            server.reservations = max(0, server.reservations - 1)

    def route(self, match_id, server_hint=""):
        server_id = self.routes.get(match_id, server_hint)
        server = self.servers.get(server_id)
        return server if server and time.monotonic() - server.seen_at <= self.stale_seconds else None
