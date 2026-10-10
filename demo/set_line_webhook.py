#!/usr/bin/env python3
"""Set and verify the LINE channel's webhook URL (Messaging API).

Usage: LINE_CHANNEL_ACCESS_TOKEN=... python3 set_line_webhook.py https://host/line/webhook
The "Use webhook" switch has no API: turn it on once in the LINE Developers Console.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

API = "https://api.line.me/v2/bot/channel/webhook"


def call(method, path, token, body=None):
    req = urllib.request.Request(
        API + path,
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as res:
            raw = res.read()
            return res.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        return e.code, {"error": e.read().decode(errors="replace")[:300]}


def main():
    if len(sys.argv) != 2 or not sys.argv[1].startswith("https://"):
        sys.exit("usage: set_line_webhook.py https://host/line/webhook")
    url = sys.argv[1]
    token = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
    if not token or token == "unset":
        sys.exit("LINE_CHANNEL_ACCESS_TOKEN is not set")

    status, body = call("PUT", "/endpoint", token, {"endpoint": url})
    if status != 200:
        sys.exit(f"Setting the webhook URL failed: HTTP {status} {body}")
    print(f"Webhook URL set: {url}")

    # A fresh quick-tunnel hostname can take a few seconds to resolve.
    result = {}
    for _ in range(10):
        status, result = call("POST", "/test", token, {"endpoint": url})
        if status == 200 and result.get("success"):
            break
        time.sleep(3)
    if result.get("success"):
        print("LINE webhook test: OK (the worker answered 200)")
    else:
        print(f"LINE webhook test FAILED: {result}", file=sys.stderr)

    status, info = call("GET", "/endpoint", token)
    if status == 200 and not info.get("active"):
        print("NOTE: 'Use webhook' is OFF. Turn it on in LINE Developers Console > Messaging API, "
              "or button taps will not reach the worker.", file=sys.stderr)
    elif status == 200:
        print("'Use webhook' is ON.")
    if not result.get("success"):
        sys.exit(1)


if __name__ == "__main__":
    main()
