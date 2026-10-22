#!/usr/bin/env python
"""重启恢复验证：before 记录状态，重启服务后 after 比对。

用法：python scripts/restart_check.py before|after
"""
import json
import os
import sys
import urllib.request

BASE = "http://127.0.0.1:9010"
FILE = os.path.join(os.path.dirname(__file__), "restart_state.json")
USER = "persist_check"
PWD = "persist123"


def call(method, path, body=None, token=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def login_or_register():
    st, body = call("POST", "/api/auth/login", {"username": USER, "password": PWD})
    if st != 200:
        st, body = call("POST", "/api/auth/register", {"username": USER, "password": PWD})
    return body["token"]


def snapshot(token):
    _, acct = call("GET", "/api/me/account", token=token)
    _, orders = call("GET", "/api/me/orders", token=token)
    _, trades = call("GET", "/api/me/trades", token=token)
    return {
        "cash_available": acct["cash_available"],
        "cash_frozen": acct["cash_frozen"],
        "total_asset": acct["total_asset"],
        "positions": [
            {"code": p["code"], "total": p["total"], "available": p["available"], "cost": p["cost"]}
            for p in acct["positions"]
        ],
        "orders": [
            {"id": o["id"], "status": o["status"], "filled": o["filled"], "reason": o["reason"]}
            for o in orders
        ],
        "trades": [{"id": t["id"], "qty": t["qty"], "price": t["price"], "fee": t["commission"] + t["stamp_tax"]} for t in trades],
    }


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "before"
    token = login_or_register()
    if mode == "before":
        st, quote = 0, None
        _, quotes = call("GET", "/api/quotes?codes=000001", token=token)
        quote = quotes[0]
        ask = quote["asks"][0]["price"]
        bid5 = quote["bids"][-1]["price"]
        _, filled = call("POST", "/api/orders", {"code": "000001", "side": "buy", "price": ask, "qty": 300}, token)
        _, pending = call("POST", "/api/orders", {"code": "000001", "side": "buy", "price": bid5 - 100, "qty": 200}, token)
        print("建仓单:", filled["status"], filled["filled"], "占位挂单:", pending["status"], "前方排队", pending["queue_ahead"])
        state = snapshot(token)
        json.dump(state, open(FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        print("已记录重启前状态:", json.dumps({k: state[k] for k in ("cash_available", "cash_frozen", "positions")}, ensure_ascii=False))
        print("委托:", [(o["status"], o["filled"]) for o in state["orders"]])
    else:
        after = snapshot(token)
        before = json.load(open(FILE, encoding="utf-8"))
        print("重启后:", json.dumps({k: after[k] for k in ("cash_available", "cash_frozen", "positions")}, ensure_ascii=False))
        print("委托:", [(o["status"], o["filled"], o["reason"]) for o in after["orders"]])
        pos_ok = [p["total"] for p in before["positions"]] == [p["total"] for p in after["positions"]]
        cost_ok = [p["cost"] for p in before["positions"]] == [p["cost"] for p in after["positions"]]
        cancel_ok = all(o["status"] != "pending" for o in after["orders"])
        cash_ok = after["cash_available"] == before["cash_available"] + before["cash_frozen"]
        print("持仓一致=%s 成本一致=%s 挂单已撤=%s 冻结资金已退回=%s" % (pos_ok, cost_ok, cancel_ok, cash_ok))
        if not (pos_ok and cost_ok and cancel_ok and cash_ok):
            sys.exit(1)


if __name__ == "__main__":
    main()
