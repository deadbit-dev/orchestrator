# orchestrator

Matchmaking, match-server registry, Scrabble persistence adapter, and the public Caddy entry point.

The generic database is external and configured through `DB_SERVICE_URL`. Match servers register themselves over the orchestrator WebSocket. Caddy exposes the player WebSocket, the anonymous queue snapshot, language packs, and match WebSockets.

## Run

```bash
cp .env.example .env
docker network create scrabble 2>/dev/null || true
docker compose --env-file .env up -d --build
```

The `db` and `match-server` containers must join `SCRABBLE_NETWORK`. The orchestrator ports are bound to localhost for administration; public traffic goes through Caddy on ports 80/443.

## Test

```bash
python -m unittest -v
docker compose --env-file .env.example config --quiet
```

`ORCHESTRATOR_MODES=ranked` enables the built-in ranked/default policy. Set it to an empty value to disable matchmaking modes without changing the routing core.
