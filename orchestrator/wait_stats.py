"""Observed matchmaking waits, used to estimate how long a new ticket will wait."""
import statistics
from collections import deque

HISTORY_SECONDS = 30 * 60
HISTORY_SAMPLES = 50
BOT_POOL_STALE_SECONDS = 15.0
# pool loop tick + lobby connect + hello, on top of the queue poll interval
BOT_JOIN_OVERHEAD_SECONDS = 2.0


def ticket_key(item):
    return item["mode"], item["language"], item["rules_version"]


class WaitStats:
    def __init__(self):
        self.human_waits, self.bot_delays = {}, {}
        self.last_bot_poll, self.bot_poll_interval = None, None

    def note_bot_poll(self, now):
        """The bot pool is the only client of the queue snapshot: its polls prove it is alive."""
        if self.last_bot_poll is not None and now > self.last_bot_poll:
            gap = now - self.last_bot_poll
            self.bot_poll_interval = gap if self.bot_poll_interval is None else 0.8 * self.bot_poll_interval + 0.2 * gap
        self.last_bot_poll = now

    def bots_online(self, now):
        if self.last_bot_poll is None:
            return False
        return now - self.last_bot_poll <= max(BOT_POOL_STALE_SECONDS, 3 * (self.bot_poll_interval or 0))

    def record_pair(self, first, second, now):
        tiers = {first["queue_tier"], second["queue_tier"]}
        for item in (first, second):
            if item["queue_tier"] != "regular":
                continue
            wait = max(0.0, now - item["queued_at"])
            if tiers == {"regular"}:
                self._push(self.human_waits, ticket_key(item), now, wait)
            else:
                self._push(self.bot_delays, ticket_key(item), now, max(0.0, wait - item["fallback_seconds"]))

    @staticmethod
    def _push(store, key, now, value):
        store.setdefault(key, deque(maxlen=HISTORY_SAMPLES)).append((now, value))

    @staticmethod
    def _median(store, key, now):
        values = [value for at, value in store.get(key, ()) if now - at <= HISTORY_SECONDS]
        return statistics.median(values) if values else None

    def bot_delay(self, key, now):
        observed = self._median(self.bot_delays, key, now)
        if observed is not None:
            return observed
        return (self.bot_poll_interval or 0) + BOT_JOIN_OVERHEAD_SECONDS

    def estimate(self, ticket, pair_eta, now):
        """Expected total wait of a regular ticket in seconds, or None when nothing predicts it."""
        wait = max(0.0, now - ticket["queued_at"])
        remaining = [] if pair_eta is None else [pair_eta]
        key = ticket_key(ticket)
        if self.bots_online(now):
            delay = self.bot_delay(key, now)
            bot_ready = ticket["fallback_seconds"] + delay
            # a bot this late is busy or does not speak the language: stop promising it
            if wait <= bot_ready + max(5.0, 2 * delay):
                remaining.append(max(0.0, bot_ready - wait))
        typical = self._median(self.human_waits, key, now)
        if typical is not None and typical > wait:
            remaining.append(typical - wait)
        return wait + min(remaining) if remaining else None
