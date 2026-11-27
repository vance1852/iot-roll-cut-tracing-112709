"""校验回放文件和回环口上的原始消息。"""

import json
from pathlib import Path
import socket
import tempfile
import threading
import unittest

from wire import ReplayServer, encode, load_trace


class EquipmentReplayTests(unittest.TestCase):
    def setUp(self):
        self.events = load_trace("fixtures/shift.ndjson")

    def test_catalog_references(self):
        catalog = json.loads(Path("fixtures/equipment.json").read_text(encoding="utf-8"))
        self.assertEqual(set(catalog["peers"]), {event["peer"] for event in self.events})
        self.assertGreater(len({event["kind"] for event in self.events}), 5)

    def test_read_ranges_and_reconnect(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "commands.ndjson"
            with ReplayServer(0, self.events, path) as server:
                worker = threading.Thread(target=server.serve_forever, args=(0.01,), daemon=True)
                worker.start()
                try:
                    with socket.create_connection(server.server_address, timeout=2) as client:
                        request = {"op": "read", "from": 0, "limit": 3}
                        with client.makefile("rwb") as stream:
                            stream.write(encode(request))
                            stream.flush()
                            self.assertEqual(json.loads(stream.readline())["events"], self.events[:3])
                    with socket.create_connection(server.server_address, timeout=2) as client:
                        with client.makefile("rwb") as stream:
                            message = {"peer": self.events[0]["peer"], "body": {"request": "status"}}
                            stream.write(encode({"op": "read", "from": 3, "limit": 500}))
                            stream.write(encode({"op": "write", "message": message}))
                            stream.flush()
                            self.assertEqual(json.loads(stream.readline())["events"], self.events[3:])
                            self.assertEqual(json.loads(stream.readline()), {"recorded": True})
                    self.assertEqual(path.read_bytes(), encode(message))
                finally:
                    server.shutdown()
                    worker.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
