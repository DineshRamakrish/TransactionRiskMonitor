import json
import http.client
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from main import Handler, RiskMonitor


class RiskMonitorTests(unittest.TestCase):
    def setUp(self):
        self.monitor = RiskMonitor(high_amount=5000, velocity_limit=3, window_seconds=60)

    def test_flags_high_amount(self):
        result, replay = self.monitor.assess({
            "transactionId": "large-1", "accountId": "customer-1", "amount": 5000
        })
        self.assertFalse(replay)
        self.assertEqual(result["decision"], "review")
        self.assertIn("high_amount", result["reasons"])

    def test_flags_account_velocity(self):
        for number in range(1, 4):
            result, _ = self.monitor.assess({
                "transactionId": f"tx-{number}", "accountId": "customer-1", "amount": 10
            })
        self.assertEqual(result["decision"], "review")
        self.assertIn("high_transaction_velocity", result["reasons"])

    def test_duplicate_id_is_idempotent(self):
        request = {"transactionId": "same-id", "accountId": "customer-1", "amount": 50}
        first, _ = self.monitor.assess(request)
        replay, was_replay = self.monitor.assess(request)
        self.assertTrue(was_replay)
        self.assertEqual(first, replay)
        self.assertEqual(len(self.monitor.transactions), 1)

    def test_rejects_non_positive_amount(self):
        with self.assertRaises(ValueError):
            self.monitor.assess({
                "transactionId": "bad-1", "accountId": "customer-1", "amount": 0
            })

    def test_rejects_non_finite_amount(self):
        with self.assertRaises(ValueError):
            self.monitor.assess({
                "transactionId": "bad-nan", "accountId": "customer-1", "amount": float("nan")
            })


class HttpApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def test_post_transaction_and_read_alerts(self):
        body = json.dumps({
            "transactionId": "http-high-amount",
            "accountId": "http-customer",
            "amount": 6000,
            "currency": "USD",
        }).encode("utf-8")
        request = Request(
            f"{self.base_url}/transactions",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=3) as response:
            assessment = json.load(response)
            self.assertEqual(response.status, 201)
        self.assertEqual(assessment["decision"], "review")
        with urlopen(f"{self.base_url}/alerts", timeout=3) as response:
            alerts = json.load(response)
        self.assertIn("http-high-amount", [alert["transactionId"] for alert in alerts])

    def test_invalid_payload_returns_bad_request(self):
        request = Request(
            f"{self.base_url}/transactions",
            data=b'{"transactionId":"bad-http"}',
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with self.assertRaises(HTTPError) as error:
            urlopen(request, timeout=3)
        self.assertEqual(error.exception.code, 400)

    def test_assessments_stream_to_connected_clients(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_address[1], timeout=3)
        try:
            connection.request("GET", "/events")
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertIn("text/event-stream", response.getheader("Content-Type"))
            self.assertEqual(response.readline(), b"retry: 3000\n")
            self.assertEqual(response.readline(), b"\n")

            body = json.dumps({
                "transactionId": "sse-high-amount",
                "accountId": "sse-customer",
                "amount": 6000,
            }).encode("utf-8")
            request = Request(
                f"{self.base_url}/transactions",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(request, timeout=3) as posted:
                self.assertEqual(posted.status, 201)

            self.assertEqual(response.readline(), b"event: assessment\n")
            payload = response.readline().decode("utf-8")
            self.assertIn('"transactionId":"sse-high-amount"', payload)
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
