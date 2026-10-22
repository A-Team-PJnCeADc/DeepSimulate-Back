#!/usr/bin/env python
"""最小 WebSocket 探针：握手、订阅、统计推送类型。仅用于本地联调。"""
import base64
import json
import os
import socket
import struct
import sys
import time
import urllib.request

BASE = "http://127.0.0.1:9010"
WS_HOST, WS_PORT = "127.0.0.1", 9010


def http(method, path, body=None, token=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    with urllib.request.urlopen(req, timeout=20) as resp:
        raw = resp.read().decode()
        return json.loads(raw) if raw else None


class WS:
    def __init__(self, token):
        self.sock = socket.create_connection((WS_HOST, WS_PORT), timeout=10)
        key = base64.b64encode(os.urandom(16)).decode()
        req = (
            "GET /ws?token=%s HTTP/1.1\r\nHost: %s:%d\r\nUpgrade: websocket\r\n"
            "Connection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n"
            % (token, WS_HOST, WS_PORT, key)
        )
        self.sock.sendall(req.encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise RuntimeError("握手失败")
            buf += chunk
        head, rest = buf.split(b"\r\n\r\n", 1)
        if b"101" not in head.split(b"\r\n")[0]:
            raise RuntimeError("握手未升级: %s" % head[:200])
        self.buf = rest

    def send(self, obj):
        payload = json.dumps(obj).encode()
        mask = os.urandom(4)
        header = bytearray([0x81])
        n = len(payload)
        if n < 126:
            header.append(0x80 | n)
        elif n < 65536:
            header.append(0x80 | 126)
            header += struct.pack(">H", n)
        else:
            header.append(0x80 | 127)
            header += struct.pack(">Q", n)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(bytes(header) + mask + masked)

    def _read(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise RuntimeError("连接关闭")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def recv(self):
        b0, b1 = self._read(2)
        opcode = b0 & 0x0F
        length = b1 & 0x7F
        if length == 126:
            length = struct.unpack(">H", self._read(2))[0]
        elif length == 127:
            length = struct.unpack(">Q", self._read(8))[0]
        data = self._read(length)
        if opcode == 0x8:
            raise RuntimeError("服务端关闭连接")
        if opcode == 0x1:
            return json.loads(data.decode())
        return None


def main():
    name = "wsprobe_%d" % int(time.time())
    auth = http("POST", "/api/auth/register", {"username": name, "password": "demo123456"})
    token = auth["token"]
    print("registered", auth["user_id"])

    ws = WS(token)
    ws.send({"type": "subscribe", "codes": ["000001", "600519"]})
    print("subscribed")

    quotes = 0
    ticks = 0
    orders = 0
    accounts = 0
    sample = {}
    deadline = time.time() + 6
    quote = None
    while time.time() < deadline:
        msg = ws.recv()
        if not msg:
            continue
        kind = msg.get("type")
        sample.setdefault(kind, msg)
        if kind == "quote":
            quotes += 1
            quote = msg["data"]
        elif kind == "tick":
            ticks += 1
        elif kind == "order":
            orders += 1
        elif kind == "account":
            accounts += 1
        if quotes == 2 and "ask" not in sample:
            sample["ask"] = 1
            d = quote
            print("首条 quote:", d["code"], d["name"], "最新", d["last"] / 1000,
                  "买一", d["bids"][0], "卖一", d["asks"][0])
            o = http("POST", "/api/orders", {"code": d["code"], "side": "buy",
                                             "price": d["asks"][0]["price"], "qty": 100}, token)
            print("下单:", o["status"], o["filled"], o["id"])

    print("消息统计: quote=%d tick=%d order=%d account=%d" % (quotes, ticks, orders, accounts))
    st = http("GET", "/api/status")
    print("status:", json.dumps(st["market"], ensure_ascii=False), "pending", st["pending_orders"], "ws", st["ws_clients"])


if __name__ == "__main__":
    main()
