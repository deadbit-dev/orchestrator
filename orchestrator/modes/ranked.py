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
