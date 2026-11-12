"""Serve the local site preview, including readable Markdown responses."""
import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class PreviewHandler(SimpleHTTPRequestHandler):
    extensions_map = {
        **SimpleHTTPRequestHandler.extensions_map,
        ".md": "text/plain; charset=utf-8",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()
    directory = Path(__file__).resolve().parents[1] / "bin/docs-web"
    handler = partial(PreviewHandler, directory=str(directory))
    with ThreadingHTTPServer(("127.0.0.1", args.port), handler) as server:
        print(f"Preview at http://127.0.0.1:{args.port}/porcupine/docs/", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
