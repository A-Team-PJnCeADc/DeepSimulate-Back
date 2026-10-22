#!/usr/bin/env python
"""端到端联调用例：注册、行情、下单、撤单、账户与委托查询。"""
import json
import sys
import time
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:9010"


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
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, raw


def main():
    name = "demo_%d" % int(time.time())
    st, r = call("POST", "/api/auth/register", {"username": name, "password": "demo123456"})
    print("register", st, r)
    if st != 200:
        sys.exit(1)
    token = r["token"]

    st, q = call("GET", "/api/quotes?codes=000001", token=token)
    print("quotes", st, json.dumps(q, ensure_ascii=False)[:400])
    if not q:
        print("行情未就绪")
        sys.exit(2)
    qt = q[0]
    ask1 = qt["asks"][0]["price"]
    bid1 = qt["bids"][0]["price"]
    print("last=%s ask1=%s(%s股) bid1=%s(%s股) 涨停=%s 跌停=%s" % (
        qt["last"] / 1000, ask1 / 1000, qt["asks"][0]["volume"],
        bid1 / 1000, qt["bids"][0]["volume"], qt["limit_up"] / 1000, qt["limit_down"] / 1000))

    st, o = call("POST", "/api/orders", {"code": "000001", "side": "buy", "price": ask1, "qty": 200}, token)
    print("买吃卖一", st, json.dumps(o, ensure_ascii=False))

    st, o2 = call("POST", "/api/orders", {"code": "000001", "side": "buy", "price": bid1 - 50, "qty": 100}, token)
    print("挂低价排队", st, json.dumps(o2, ensure_ascii=False))

    st, o3 = call("POST", "/api/orders", {"code": "000001", "side": "buy", "price": qt["limit_up"] + 10, "qty": 100}, token)
    print("涨停之上废单", st, json.dumps(o3, ensure_ascii=False))

    st, o4 = call("POST", "/api/orders", {"code": "000001", "side": "buy", "price": bid1, "qty": 150}, token)
    print("整手校验", st, json.dumps(o4, ensure_ascii=False))

    st, o5 = call("POST", "/api/orders", {"code": "000001", "side": "sell", "price": bid1, "qty": 100}, token)
    print("T+1 不可卖", st, json.dumps(o5, ensure_ascii=False))

    time.sleep(0.3)
    st, acct = call("GET", "/api/me/account", token=token)
    print("account", st, json.dumps(acct, ensure_ascii=False)[:700])

    st, orders = call("GET", "/api/me/orders", token=token)
    print("orders", st, [(x["id"], x["status"], x["filled"], x["queue_ahead"], x.get("reason")) for x in orders])

    st, trades = call("GET", "/api/me/trades", token=token)
    print("trades", st, [(t["price"], t["qty"], t["commission"], t["stamp_tax"]) for t in trades])

    if o2.get("status") in ("pending", "partial"):
        st, c = call("DELETE", "/api/orders/" + o2["id"], token=token)
        print("cancel", st, json.dumps(c, ensure_ascii=False))

    st, acct = call("GET", "/api/me/account", token=token)
    print("account after cancel", json.dumps({k: acct[k] for k in ("cash_available", "cash_frozen", "market_value", "total_asset")}, ensure_ascii=False))

    st, mi = call("GET", "/api/quotes/000001/minute", token=token)
    print("minute", st, (len(mi["points"]) if isinstance(mi, dict) else mi), "prev_close", mi.get("prev_close") if isinstance(mi, dict) else None)
    st, tk = call("GET", "/api/quotes/000001/ticks", token=token)
    print("ticks", st, len(tk) if isinstance(tk, list) else tk)
    st, status = call("GET", "/api/status")
    print("status", st, json.dumps(status, ensure_ascii=False)[:400])
    print("TOKEN", token)


if __name__ == "__main__":
    main()
