"""Small HTTP transport for the internal queue snapshot endpoint."""
import asyncio
import json

MAX_HEADER_BYTES = 16 * 1024
MAX_BODY_BYTES = 256 * 1024


async def respond(writer, status, value):
    body = json.dumps(value, separators=(",", ":")).encode()
    writer.write(("HTTP/1.1 %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n" % (status, len(body))).encode() + body)
    await writer.drain()


async def queue_http_handler(reader, writer, app):
    try:
        request = await reader.readuntil(b"\r\n\r\n")
        head = request.decode("iso-8859-1").split("\r\n")
        method, path, _ = head[0].split(" ", 2)
        if not all(":" in line for line in head[1:] if line):
            raise ValueError
        headers = dict(line.split(":", 1) for line in head[1:] if ":" in line)
        length = int(headers.get("Content-Length", headers.get("content-length", "0")))
        if length < 0 or length > MAX_BODY_BYTES:
            raise ValueError
        body = await reader.readexactly(length) if length else b""
        status, value = await app.api_response(method, path, headers, body)
        await respond(writer, status, value)
    except (ValueError, UnicodeDecodeError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
        try:
            await respond(writer, "400 Bad Request", {"error": "bad_request"})
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except ConnectionError:
            pass


async def serve_queue_api(app, host, port):
    return await asyncio.start_server(lambda reader, writer: queue_http_handler(reader, writer, app), host, port, limit=MAX_HEADER_BYTES)
