"""回放本地试验台消息并记录发往设备的内容。"""

import argparse
from collections import Counter
import json
from pathlib import Path
from socketserver import StreamRequestHandler, TCPServer


def encode(value):
    """生成一行 UTF-8 消息。"""
    return (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")


def load_trace(path):
    """读取并检查回放文件的外层消息格式。"""
    with Path(path).open(encoding="utf-8") as stream:
        events = [json.loads(line) for line in stream]
    for offset, event in enumerate(events):
        if (
            event["offset"] != offset
            or not isinstance(event["peer"], str)
            or not isinstance(event["kind"], str)
            or not isinstance(event["body"], dict)
        ):
            raise ValueError(f"消息外层格式有误，位置 {offset}")
    return events


class ReplayHandler(StreamRequestHandler):
    """接收回放请求或需要记录的外发信封。"""

    def handle(self):
        for line in self.rfile:
            try:
                message = json.loads(line)
                response = self.respond(message)
            except (ValueError, KeyError, TypeError, AttributeError) as error:
                response = {"error": str(error)}
            self.wfile.write(encode(response))

    def respond(self, message):
        operation = message["op"]
        if operation == "read":
            start = message["from"]
            limit = message["limit"]
            if type(start) is not int or start < 0 or type(limit) is not int or not 1 <= limit <= 500:
                raise ValueError("from 要求非负整数，limit 要求 1 到 500")
            return {"events": self.server.events[start : start + limit]}
        if operation == "write":
            envelope = message["message"]
            if envelope["peer"] not in self.server.peers or not isinstance(envelope["body"], dict):
                raise ValueError("请选择已登记设备并提供消息正文")
            with self.server.capture.open("ab") as stream:
                stream.write(encode(envelope))
            return {"recorded": True}
        raise ValueError(f"未知回放操作 {operation}")


class ReplayServer(TCPServer):
    """绑定本机回环口的试验台。"""

    def __init__(self, port, events, capture):
        super().__init__(("127.0.0.1", port), ReplayHandler)
        self.events = events
        self.peers = {event["peer"] for event in events}
        self.capture = Path(capture)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace", type=Path, default=Path("fixtures/shift.ndjson"))
    subparsers = parser.add_subparsers(dest="action", required=True)
    subparsers.add_parser("inspect", help="统计输入消息")
    serve = subparsers.add_parser("serve", help="开启本机回放端")
    serve.add_argument("--port", type=int, default=9040)
    serve.add_argument("--capture", type=Path, default=Path("capture.ndjson"))
    args = parser.parse_args()
    events = load_trace(args.trace)
    if args.action == "inspect":
        print(json.dumps({"消息数": len(events), "节点": Counter(event["peer"] for event in events)},
                         ensure_ascii=False, sort_keys=True))
        return
    with ReplayServer(args.port, events, args.capture) as server:
        print(f"监听 127.0.0.1:{server.server_address[1]}", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
