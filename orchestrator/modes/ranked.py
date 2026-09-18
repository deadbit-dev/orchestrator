"""The built-in ranked/default matchmaking policy."""


def rating_gap(wait_seconds):
    return min(400, 100 + 50 * int(max(0.0, wait_seconds) // 15.0))


def _same(first, second):
    return (first["mode"], first["language"], first["rules_version"]) == (second["mode"], second["language"], second["rules_version"])


def pairs(entries, now, fallback_seconds):
    # ponytail: O(n²) is enough for the current small matchmaking queue; index by rating if it grows.
    regular = [item for item in entries if item["queue_tier"] == "regular"]
    fallback = [item for item in entries if item["queue_tier"] == "fallback"]
    result = []

    def take_regular(predicate):
        index = 0
        while index < len(regular) - 1:
            first = regular[index]
            best = min((candidate_index for candidate_index in range(index + 1, len(regular))
                        if _same(first, regular[candidate_index]) and predicate(first, regular[candidate_index])),
                       key=lambda candidate_index: (abs(first["rating"] - regular[candidate_index]["rating"]), regular[candidate_index]["queued_at"]), default=None)
            if best is None:
                index += 1
            else:
                result.append((regular.pop(index), regular.pop(best - 1)))

    take_regular(lambda first, second: abs(first["rating"] - second["rating"]) <= max(rating_gap(now - first["queued_at"]), rating_gap(now - second["queued_at"])))
    take_regular(lambda first, second: max(now - first["queued_at"], now - second["queued_at"]) >= fallback_seconds)
    index = 0
    while index < len(regular):
        first = regular[index]
        if now - first["queued_at"] < fallback_seconds:
            index += 1
            continue
        candidate = next((i for i, item in enumerate(fallback) if _same(first, item)), None)
        if candidate is None:
            index += 1
        else:
            result.append((regular.pop(index), fallback.pop(candidate)))
    return result, sorted(regular + fallback, key=lambda item: item["queued_at"])


def _rating_wait(difference):
    """Wait, in seconds, after which rating_gap() admits this rating difference."""
    if difference <= 100:
        return 0.0
    if difference > 400:
        return None
    return 15.0 * -(-(difference - 100) // 50)


def pair_eta(ticket, entries, now, fallback_seconds):
    """Seconds until `ticket` pairs with a compatible ticket already queued, or None."""
    wait = now - ticket["queued_at"]
    best = None
    for other in entries:
        if other is ticket or other["profile_id"] == ticket["profile_id"] or not _same(ticket, other):
            continue
        if other["queue_tier"] == "fallback":
            eta = fallback_seconds - wait
        else:
            longest = max(wait, now - other["queued_at"])
            eta = fallback_seconds - longest
            rating_wait = _rating_wait(abs(ticket["rating"] - other["rating"]))
            if rating_wait is not None:
                eta = min(eta, rating_wait - longest)
        eta = max(0.0, eta)
        best = eta if best is None else min(best, eta)
    return best
