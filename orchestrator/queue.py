"""Queue tickets, anonymised snapshots, and mode dispatch."""
import time
import secrets
from .modes import enabled


class Queue:
    def __init__(self, modes=None):
        self.entries = []
        self.revision = 0
        self.modes = enabled() if modes is None else modes

    def queue_player(self, item):
        item.setdefault("queue_tier", "regular")
        item.setdefault("ticket_id", secrets.token_urlsafe(12))
        item.setdefault("fallback_seconds", 10.0)
        self.entries = [entry for entry in self.entries if entry["profile_id"] != item["profile_id"]]
        self.entries.append(item)
        self.revision += 1

    def append(self, item):
        self.queue_player(item)

    def __iter__(self):
        return iter(self.entries)

    def __len__(self):
        return len(self.entries)

    def __eq__(self, other):
        return self.entries == other

    def cancel_player(self, profile_id):
        before = len(self.entries)
        self.entries = [entry for entry in self.entries if entry["profile_id"] != profile_id]
        if len(self.entries) != before:
            self.revision += 1

    def pairs(self, now=None, fallback_seconds=10.0):
        now = time.time() if now is None else now
        selected, remaining = [], []
        for mode, module in self.modes.items():
            matching = [entry for entry in self.entries if entry["mode"] == mode]
            other = [entry for entry in self.entries if entry["mode"] != mode]
            pairs, matching = module.pairs(matching, now, fallback_seconds)
            selected.extend(pairs); self.entries = other + matching
        self.entries.sort(key=lambda item: item["queued_at"])
        if selected:
            self.revision += 1
        return selected

    def snapshot(self, now=None):
        now = time.time() if now is None else now
        return {"revision": self.revision, "server_time": now, "entries": [
            {"ticket_id": item["ticket_id"], "mode": item["mode"], "language": item["language"],
             "rules_version": item["rules_version"], "rating": item["rating"],
             "wait_seconds": max(0.0, now - item["queued_at"]),
             "fallback_eligible": item["queue_tier"] == "regular" and now - item["queued_at"] >= item["fallback_seconds"]}
            for item in self.entries]}
