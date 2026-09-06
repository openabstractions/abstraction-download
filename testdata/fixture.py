"""The internet, made deterministic.

Ten scenarios compared three implementations and every one of them read a file.
No replay driver could make an HTTP request, so the wire behaviour of three
independently written HTTP clients had never been compared at all -- and that is
the surface facing the internet.

This serves the answers a real server gives and a contract has to survive: a
range honoured, a range ignored, a 416, a 206 starting somewhere else, a
Content-Range that lies about the body, a span shorter than the one asked for, a
body that disagrees with its own Content-Length, a redirect, a refusal, a version
that changed under a resume.

Behaviour is a function of the URL alone, so a request carries its own answer and
nothing depends on what came before it. State would make a scenario depend on the
order three drivers happened to run in, and a fixture that occasionally fails
teaches everyone to ignore red.

    python fixture.py

prints one line, the base URL, and serves until killed. A driver builds
`<base>/<run>/<behaviour>/<size>`; `<run>` is any token, and it exists so two
drivers reading the same behaviour cannot see each other.
"""

import gzip
import socketserver
import sys

REASON = {
    200: "OK", 206: "Partial Content", 302: "Found", 304: "Not Modified",
    400: "Bad Request", 401: "Unauthorized", 403: "Forbidden", 404: "Not Found",
    408: "Request Timeout", 410: "Gone", 416: "Range Not Satisfiable",
    418: "I'm a teapot", 429: "Too Many Requests", 451: "Unavailable For Legal Reasons",
    500: "Internal Server Error", 503: "Service Unavailable",
}


# The one date spelling RFC 9110 requires a server to send, which is what the
# `modified` behaviour hands back as the version it is now serving.
MODIFIED = "Mon, 07 Nov 1994 08:49:37 GMT"

# And one of the two spellings RFC 9110 requires a RECIPIENT to accept, which
# real servers still send and which `obsoletedate` answers with. Recording it or
# dropping it is the difference between a resume that can name its version and
# one that cannot, and nothing else in a transcript tells the two apart.
OBSOLETE = "Sunday, 06-Nov-94 08:49:37 GMT"

# The token the `gated` behaviour admits. A driver holds it under the credential
# name a scenario gives, so what arrives on this wire is what the runner chose
# to send — the whole question the deputy scenarios ask.
CANARY = "hf_thisMustNeverAppearOnDisk_EXAMPLE"


def artifact(n):
    return bytes(i % 251 for i in range(n))


def range_start(value):
    """The first byte position of a `bytes=N-` or `bytes=N-M` request, or None."""
    if not value.startswith("bytes="):
        return None
    first = value[len("bytes="):].split(",")[0].split("-")[0].strip()
    return int(first) if first.isdigit() else None


class Wire(socketserver.StreamRequestHandler):
    def handle(self):
        request = self.rfile.readline(65536).decode("latin-1").split()
        if len(request) < 2:
            return
        headers = {}
        while True:
            line = self.rfile.readline(65536)
            if line in (b"\r\n", b"\n", b""):
                break
            name, _, value = line.decode("latin-1").partition(":")
            headers[name.strip().lower()] = value.strip()
        try:
            self.answer(request[1].split("?")[0], headers)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def send(self, status, headers, body, length=None):
        """Every response closes the connection.

        Keep-alive would make a scenario's answers depend on which requests a
        client chose to put on one socket, and two of the behaviours below are
        defined by closing early -- which has no meaning on a reused connection.
        """
        head = "HTTP/1.1 %d %s\r\n" % (status, REASON.get(status, "Status"))
        if length is not None:
            head += "Content-Length: %d\r\n" % length
        head += "Accept-Ranges: bytes\r\nConnection: close\r\n"
        for name, value in headers.items():
            head += "%s: %s\r\n" % (name, value)
        self.wfile.write(head.encode("latin-1") + b"\r\n" + body)
        self.wfile.flush()

    def answer(self, path, headers):
        parts = [p for p in path.split("/") if p]
        if len(parts) < 3 or not parts[-1].isdigit():
            self.send(404, {}, b"", 0)
            return
        behaviour, size = parts[-2], int(parts[-1])
        body = artifact(size)
        start = range_start(headers.get("range", ""))
        whole = {"Content-Type": "application/octet-stream"}

        def spanned(first, last, total="%d" % size):
            return dict(whole, **{"Content-Range": "bytes %d-%d/%s" % (first, last, total)})

        if behaviour == "gated":
            if headers.get("authorization") != "Bearer " + CANARY:
                self.send(401, whole, b"", 0)
                return
            behaviour = "plain"

        if behaviour.startswith("status-"):
            self.send(int(behaviour[len("status-"):]), whole, b"", 0)
        elif behaviour == "redirect":
            self.send(302, {"Location": "/".join(path.split("/")[:-2] + ["plain", str(size)])}, b"", 0)
        elif behaviour == "norange":
            self.send(200, whole, body, size)
        elif behaviour == "unranged206":
            self.send(206, spanned(0, size - 1), body, size)
        elif behaviour == "gzip":
            packed = gzip.compress(body, mtime=0)
            self.send(200, dict(whole, **{"Content-Encoding": "gzip"}), packed, len(packed))
        elif behaviour == "truncate":
            self.send(200, whole, body[: size // 2], size)
        elif behaviour == "overlong":
            self.send(200, whole, body + b"\x00" * 8, size)
        elif behaviour == "nolength":
            self.send(200, whole, body[: size // 2])
        elif behaviour == "obsoletedate":
            self.send(200, dict(whole, **{"Last-Modified": OBSOLETE}), body, size)
        elif start is None:
            self.send(200, whole, body, size)
        elif behaviour == "range416":
            self.send(416, spanned(0, 0, "*"), b"", 0)
        elif behaviour == "offset":
            self.send(206, spanned(0, size - 1), body, size)
        elif behaviour == "subspan":
            last = min(start + 7, size - 1)
            self.send(206, spanned(start, last), body[start:last + 1], last + 1 - start)
        elif behaviour == "star":
            self.send(206, spanned(start, size - 1, "*"), body[start:], size - start)
        elif behaviour == "lying":
            self.send(206, spanned(start, size - 1), body[: size - start], size - start)
        elif behaviour in ("multipart", "markedmultipart"):
            part = (b"\r\n--abstraction\r\nContent-Type: application/octet-stream\r\n"
                    b"Content-Range: bytes %d-%d/%d\r\n\r\n" % (start, size - 1, size)
                    + body[start:] + b"\r\n--abstraction--\r\n")
            head = {"Content-Type": "multipart/byteranges; boundary=abstraction"}
            if behaviour == "markedmultipart":
                # The same body under a top-level Content-Range naming exactly
                # the span that was asked for. Every offset check passes and the
                # bytes are still MIME, so only the Content-Type says so.
                head["Content-Range"] = "bytes %d-%d/%d" % (start, size - 1, size)
            self.send(206, head, part, len(part))
        elif behaviour == "modified":
            if headers.get("if-range", MODIFIED) != MODIFIED:
                self.send(200, dict(whole, **{"Last-Modified": MODIFIED}), body, size)
            else:
                self.send(206, dict(spanned(start, size - 1), **{"Last-Modified": MODIFIED}),
                          body[start:], size - start)
        elif behaviour == "etag":
            if headers.get("if-range", '"v2"') != '"v2"':
                self.send(200, dict(whole, ETag='"v2"'), body, size)
            else:
                self.send(206, dict(spanned(start, size - 1), ETag='"v2"'),
                          body[start:], size - start)
        else:
            self.send(206, spanned(start, size - 1), body[start:], size - start)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = False
    daemon_threads = True


def main():
    server = Server(("127.0.0.1", 0), Wire)
    sys.stdout.write("http://127.0.0.1:%d\n" % server.server_address[1])
    sys.stdout.flush()
    server.serve_forever()


if __name__ == "__main__":
    main()
