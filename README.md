# Transactional Outbox for Go (pgx)

A Go library implementing the Transactional Outbox pattern using `pgx` (Go 1.25+). It ensures reliable message delivery from a PostgreSQL-backed application to external systems by recording messages in an outbox table within the same transaction as your business logic.

## Features

- **Transactional Integrity**: Save domain events and business data changes in a single atomic operation.
- **Reliable Delivery**: Background worker ensures messages are eventually published even if the external system is temporarily unavailable.
- **Pgx Integration**: Built specifically for `github.com/jackc/pgx/v5`.
- **Flexible Routing**: Custom resolvers to map internal events to external queue/topics/keys.

## Database Schema

The `PgxRepository` expects an `outbox_messages` table. You can use the following SQL to create it:

```sql
CREATE TYPE outbox_status AS ENUM ('PENDING', 'PROCESSING', 'PUBLISHED', 'FAILED');

CREATE TABLE IF NOT EXISTS outbox_messages
(
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    aggregate_type VARCHAR(32)   NOT NULL,
    aggregate_id   VARCHAR(36)   NOT NULL,
    event_type     VARCHAR(32)   NOT NULL,
    payload        BYTEA         NOT NULL,
    metadata       JSONB         NOT NULL DEFAULT '{}',
    status         outbox_status NOT NULL,
    error          TEXT,
    occurred_at    TIMESTAMPTZ   NOT NULL,
    created_at     TIMESTAMPTZ   NOT NULL,
    published_at   TIMESTAMPTZ
);

CREATE INDEX idx_outbox_messages_status_occurred_ready ON outbox_messages (occurred_at ASC) WHERE status IN ('PENDING', 'PROCESSING');

```

## Usage

### 1. Writing Messages

Write messages to the outbox table within your business logic transaction.

```go
import (
    "context"
    "encoding/json"
    "time"
    outbox "github.com/ivan-prykhodko/pgx-outbox"
)

func CreateOrder(ctx context.Context, pool *pgxpool.Pool, order Order) error {
    return pool.BeginFunc(ctx, func(tx pgx.Tx) error {
        // 1. Perform business logic (e.g., save order)
        // ...

        // 2. Prepare outbox message
        payload, _ := json.Marshal(order)
        msg := outbox.NewMessage(
            "Order",
            order.ID,
            "OrderCreated",
            payload,
            nil,
            time.Now(),
        )

        // 3. Write to outbox within the same transaction
        writer := outbox.NewWriter("outbox_messages")
        _, err := writer.Write(ctx, tx, &msg)
        return err
    })
}
```

### 2. Processing Messages (Worker)

Set up a background worker to poll and publish pending messages.

```go
import (
    "context"
    "time"
    outbox "github.com/ivan-prykhodko/pgx-outbox"
)

func startOutboxWorker(ctx context.Context, pool *pgxpool.Pool) {
    // 1. Initialize components
    repo := outbox.NewRepository(pool)
    publisher := &MyKafkaPublisher{} // Implements outbox.Publisher
    router := outbox.NewRouter(map[string]outbox.RouteResolver{
        outbox.RouteName("Order", "OrderCreated"): func(msg *outbox.Message) (outbox.Route, error) {
            return newMyRoute(
				"order", // Topic
			    "some-key", // Key
			    fmt.Sprintf("outbox:%s:%s:%s:%d", msg.AggregateType, msg.AggregateID, msg.EventType, msg.ID), // Idempotency key
            ), nil
        },
    })
    dispatcher := outbox.NewDispatcher(publisher, router)

    reader := outbox.NewPollReader(repo, 100) // Batch limit 100
    processor := outbox.NewDefaultProcessor(repo, dispatcher)

    // 2. Start the worker
    worker := outbox.NewWorker(
        reader,
        processor,
        1*time.Second, // Polling interval
        5*time.Second, // Sleep on error
        nil,           // Default logger
    )

    worker.Run(ctx)
}
```

## Configuration

- **Writer**: Used in your application code to insert messages.
- **Repository**: Handles fetching and updating message status in the database.
- **Reader**: Responsible for reading pending messages from the repository (e.g., via polling).
- **Processor**: Handles the processing logic for individual messages.
- **Publisher**: You must provide an implementation of the `Publisher` interface (e.g., for Kafka, RabbitMQ, or SNS).
- **Worker**: Orchestrates the polling and dispatching process using a Reader and Processor.

## Error Handling & Retries

The worker handles transient errors (like network issues) by sleeping for a configured duration before retrying. If a message fails due to a non-retryable error (e.g., routing failure), it is marked as `FAILED` with the error message recorded in the database.

## License

MIT
