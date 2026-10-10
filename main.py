import json
import os
import queue
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HIGH_AMOUNT = 5_000
VELOCITY_LIMIT = 5
VELOCITY_WINDOW_SECONDS = 60


class RiskMonitor:
    def __init__(self, high_amount=HIGH_AMOUNT, velocity_limit=VELOCITY_LIMIT, window_seconds=VELOCITY_WINDOW_SECONDS):
        self.high_amount = high_amount
        self.velocity_limit = velocity_limit
        self.window_seconds = window_seconds
        self.lock = threading.Lock()
        self.transactions = {}
        self.alerts = []
        self.account_activity = defaultdict(deque)
        self.subscribers = set()

    def assess(self, request):
        transaction_id = request["transactionId"]
        account_id = request["accountId"]
        if not transaction_id.strip() or not account_id.strip():
            raise ValueError("transactionId and accountId cannot be blank.")
        if isinstance(request["amount"], bool):
            raise ValueError("Amount must be a positive number with at most two decimals.")
        try:
            decimal_amount = Decimal(str(request["amount"]))
        except (InvalidOperation, ValueError) as error:
            raise ValueError("Amount must be a positive number with at most two decimals.") from error
        if not decimal_amount.is_finite() or decimal_amount <= 0:
            raise ValueError("Amount must be greater than zero.")
        try:
            rounded_amount = decimal_amount.quantize(Decimal("0.01"))
        except InvalidOperation as error:
            raise ValueError("Amount is too large to process.") from error
        if rounded_amount != decimal_amount:
            raise ValueError("Amount must have at most two decimals.")
        amount = float(decimal_amount)
        currency = request.get("currency", "USD")
        if not isinstance(currency, str) or len(currency) != 3 or not currency.isalpha():
            raise ValueError("Currency must be a three-letter code.")
        if currency.upper() != "USD":
            raise ValueError("This demo risk rule set supports USD only.")

        now = time.monotonic()
        with self.lock:
            previous = self.transactions.get(transaction_id)
            if previous is not None:
                return previous, True

            activity = self.account_activity[account_id]
            while activity and now - activity[0] > self.window_seconds:
                activity.popleft()
            activity.append(now)

            reasons = []
            score = 0
            if amount >= self.high_amount:
                reasons.append("high_amount")
                score += 70
            if len(activity) >= self.velocity_limit:
                reasons.append("high_transaction_velocity")
                score += 40
            score = min(score, 100)
            result = {
                "transactionId": transaction_id,
                "accountId": account_id,
                "amount": round(amount, 2),
                "currency": currency.upper(),
                "riskScore": score,
                "decision": "review" if reasons else "no_alert",
                "reasons": reasons,
                "createdAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            self.transactions[transaction_id] = result
            if reasons:
                self.alerts.append(result)
            for subscriber in tuple(self.subscribers):
                try:
                    subscriber.put_nowait(result)
                except queue.Full:
                    try:
                        subscriber.get_nowait()
                    except queue.Empty:
                        pass
                    subscriber.put_nowait(result)
            return result, False

    def get_alerts(self):
        with self.lock:
            return list(self.alerts)

    def subscribe(self):
        channel = queue.Queue(maxsize=100)
        with self.lock:
            self.subscribers.add(channel)
        return channel

    def unsubscribe(self, channel):
        with self.lock:
            self.subscribers.discard(channel)


monitor = RiskMonitor()


class Handler(BaseHTTPRequestHandler):
    server_version = "TransactionRiskMonitor/1.0"

    def do_GET(self):
        if self.path == "/health":
            self._json(200, {"status": "ok", "mode": "simulation"})
        elif self.path == "/alerts":
            self._json(200, monitor.get_alerts())
        elif self.path == "/events":
            self._events()
        else:
            self._json(404, {"error": "Route not found."})

    def do_POST(self):
        if self.path != "/transactions":
            self._json(404, {"error": "Route not found."})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 16_384:
                self._json(400, {"error": "Request body must be between 1 and 16384 bytes."})
                return
            request = json.loads(self.rfile.read(length))
            if not isinstance(request, dict):
                raise ValueError("Request body must be a JSON object.")
            for field in ("transactionId", "accountId", "amount"):
                if field not in request or request[field] in ("", None):
                    raise ValueError(f"Missing required field: {field}.")
            if not isinstance(request["transactionId"], str) or not isinstance(request["accountId"], str):
                raise ValueError("transactionId and accountId must be strings.")
            result, replay = monitor.assess(request)
            self._json(200 if replay else 201, result)
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._json(400, {"error": "Request body must be valid JSON."})
        except (ValueError, TypeError) as error:
            self._json(400, {"error": str(error)})

    def _events(self):
        channel = monitor.subscribe()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        try:
            self.wfile.write(b"retry: 3000\n\n")
            self.wfile.flush()
            while True:
                try:
                    event = channel.get(timeout=15)
                    payload = json.dumps(event, separators=(",", ":"))
                    self.wfile.write(f"event: assessment\ndata: {payload}\n\n".encode("utf-8"))
                except queue.Empty:
                    self.wfile.write(b": keep-alive\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            monitor.unsubscribe(channel)

    def _json(self, status, value):
        body = json.dumps(value, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format_string, *args):
        print(f"{self.log_date_time_string()} {format_string % args}")


def main():
    port = int(os.environ.get("PORT", "8083"))
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"Simulation-only risk monitor listening on http://127.0.0.1:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping risk monitor.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
