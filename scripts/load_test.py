#!/usr/bin/env python
"""并发下单压测：N 个用户各自下单，同时用一条 WS 连接观测行情推送是否卡顿。

用法：python scripts/load_test.py [用户数] [每人委托数]
"""
import json
import os
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = "http://127.0.0.1:9010"
USERS = int(sys.argv[1]) if len(sys.argv) > 1 else 50
ORDERS = int(sys.argv[2]) if len(sys.argv) > 2 else 10
CODE = sys.argv[3] if len(sys.argv) > 3 else "000001"


def call(method, path, body=None, token=None, timeout=20):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode()
        return json.loads(raw) if raw else None


class WsWatch(threading.Thread):
    """只读 WS：统计 quote 间隔，判断推送是否卡顿。"""

    def __init__(self, token):
        super().__init__(daemon=True)
        self.token = token
        self.quotes = 0
        self.gaps = []
        self.lock = threading.Lock()
        self.stop_flag = False

    def run(self):
        import socket, base64, struct

        sock = socket.create_connection(("127.0.0.1", 9010), timeout=15)
        key = base64.b64encode(os.urandom(16)).decode()
        sock.sendall(
            (
                "GET /ws?token=%s HTTP/1.1\r\nHost: 127.0.0.1:9010\r\nUpgrade: websocket\r\n"
                "Connection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n"
                % (self.token, key)
            ).encode()
        )
        buf = b""
        while b"\r\n\r\n" not in buf:
            buf += sock.recv(4096)
        _, buf = buf.split(b"\r\n\r\n", 1)

        payload = json.dumps({"type": "subscribe", "codes": [CODE]}).encode()
        mask = os.urandom(4)
        sock.sendall(bytes([0x81, 0x80 | len(payload)]) + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))

        last = time.perf_counter()
        sock.settimeout(2.0)
        while not self.stop_flag:
            while len(buf) < 2:
                try:
                    chunk = sock.recv(65536)
                except socket.timeout:
                    continue
                if not chunk:
                    return
                buf += chunk
            b0, b1 = buf[0], buf[1]
            ln = b1 & 0x7F
            off = 2
            if ln == 126:
                while len(buf) < 4:
                    buf += sock.recv(65536)
                ln = struct.unpack(">H", buf[2:4])[0]
                off = 4
            elif ln == 127:
                while len(buf) < 10:
                    buf += sock.recv(65536)
                ln = struct.unpack(">Q", buf[2:10])[0]
                off = 10
            while len(buf) < off + ln:
                try:
                    chunk = sock.recv(65536)
                except socket.timeout:
                    continue
                if not chunk:
                    return
                buf += chunk
            frame, buf = buf[off : off + ln], buf[off + ln :]
            if (b0 & 0x0F) != 0x1:
                continue
            try:
                msg = json.loads(frame.decode())
            except Exception:
                continue
            if msg.get("type") == "quote":
                now = time.perf_counter()
                with self.lock:
                    self.quotes += 1
                    self.gaps.append((now - last) * 1000)
                last = now
        sock.close()


def main():
    stamp = int(time.time())
    print("注册 %d 个用户…" % USERS)
    tokens = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        def reg(i):
            return call("POST", "/api/auth/register",
                        {"username": "load_%d_%d" % (stamp, i), "password": "load123456"})

        for auth in pool.map(reg, range(USERS)):
            tokens.append(auth["token"])

    quote = call("GET", "/api/quotes?codes=" + CODE)[0]
    ask = quote["asks"][0]["price"]
    bid = quote["bids"][0]["price"]
    print("最新 %.2f 卖一 %.2f 买一 %.2f" % (quote["last"] / 1000, ask / 1000, bid / 1000))

    watcher = WsWatch(tokens[0])
    watcher.start()
    time.sleep(1.0)

    latencies = []
    lat_lock = threading.Lock()
    status_count = {}

    def work(idx_token):
        _, token = idx_token
        local = []
        for k in range(ORDERS):
            # 交替：吃卖一（立即成交）与挂买一（排队）
            px = ask if k % 2 == 0 else bid
            t0 = time.perf_counter()
            try:
                o = call("POST", "/api/orders", {"code": CODE, "side": "buy", "price": px, "qty": 100}, token)
            except Exception as e:
                print("下单失败", e)
                continue
            dt = (time.perf_counter() - t0) * 1000
            local.append(dt)
            st = o.get("status", "error")
            with lat_lock:
                status_count[st] = status_count.get(st, 0) + 1
        with lat_lock:
            latencies.extend(local)

    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=USERS) as pool:
        list(pool.map(work, enumerate(tokens)))
    total = time.perf_counter() - t_start

    time.sleep(1.0)
    watcher.stop_flag = True

    latencies.sort()

    def p(q):
        return latencies[min(len(latencies) - 1, int(len(latencies) * q))]

    print("并发用户=%d 每人委托=%d 总耗时=%.2fs 吞吐=%.0f 单/秒" % (USERS, ORDERS, total, len(latencies) / total))
    print("下单延迟 p50=%.1fms p95=%.1fms p99=%.1fms max=%.1fms" % (p(0.5), p(0.95), p(0.99), latencies[-1]))
    print("委托状态分布:", status_count)
    with watcher.lock:
        gaps = watcher.gaps[1:]
        if gaps:
            print("WS 行情推送 %d 条，间隔 p50=%.0fms max=%.0fms" % (
                watcher.quotes, statistics.median(gaps), max(gaps)))
    st = call("GET", "/api/status")
    print("服务端：未完成委托=%d WS客户端=%d 撮合耗时(us)=%s" % (
        st["pending_orders"], st["ws_clients"], st["match_latency_us"]))


if __name__ == "__main__":
    main()
