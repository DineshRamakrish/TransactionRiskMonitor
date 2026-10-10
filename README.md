# Real-Time Transaction Risk Monitor

A dependency-free Python service that scores **fictional** transactions using
transparent high-amount and account-velocity rules. It returns a review
decision, lists flagged alerts, and streams new decisions over server-sent
events.

## Run

```text
python main.py
```

The service listens on `http://localhost:8083`. Set the `PORT` environment
variable to choose another port.

## Try it

```text
curl http://localhost:8083/health
curl -X POST http://localhost:8083/transactions -H "Content-Type: application/json" -d "{\"transactionId\":\"demo-001\",\"accountId\":\"customer-1\",\"amount\":6200,\"currency\":\"USD\"}"
curl http://localhost:8083/alerts
curl -N http://localhost:8083/events
```

The default rules flag an amount of USD 5,000 or more or five transactions
from one account in a 60-second window. The rules, data, and event history are
for demonstration only and are in memory. This is not a production fraud
system, AML decision, bank, or financial advice; it does not authorize,
decline, or move real payments.

Run tests with `python -m unittest -v`.
