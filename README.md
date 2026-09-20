# Transactional Outbox for Go (pgx)

A Go library implementing the Transactional Outbox pattern using `pgx` (Go 1.25+). It ensures reliable message delivery from a PostgreSQL-backed application to external systems by recording messages in an outbox table within the same transaction as your business logic.

## Features

- **Transactional Integrity**: Save domain events and business data changes in a single atomic operation.
- **Reactive Engine**: Uses PostgreSQL's `LISTEN/NOTIFY` to trigger immediate processing, reducing latency compared to polling-only approaches.
- **Reliable Delivery**: Background worker ensures messages are eventually published with fallback polling and configurable retry policies.
- **Pgx Integration**: Built specifically for `github.com/jackc/pgx/v5`.
- **Flexible Routing**: Custom resolvers to map internal events to external queue/topics/keys.
- **Flow Control**: Batch-based processing with acknowledgment ensures the system is not overwhelmed.

## Database Schema

The `PgxRepository` expects an `outbox_messages` table. You should also add a trigger to enable reactive notifications.

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

-- Optional but recommended: Trigger for reactive processing
CREATE OR REPLACE FUNCTION notify_outbox() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('outbox_events', NEW.aggregate_type::text);
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS outbox_notify_trigger ON outbox_messages;

CREATE TRIGGER outbox_notify_trigger
    AFTER INSERT ON outbox_messages
    FOR EACH ROW
    EXECUTE FUNCTION notify_outbox();
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
        payload, _ := json.Marshal(domainEvent)
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
        _, err := writer.Write(ctx, tx, &msg) // Pass by pointer
        return err
    })
}
```

### 2. Processing Messages (Worker)

Set up a reactive background worker using a `Provider` and `NotificationListener`.

```go
import (
    "context"
    "log"
    "time"
    outbox "github.com/ivan-prykhodko/pgx-outbox"
)

func startOutboxWorker(ctx context.Context, pool *pgxpool.Pool) {
    repo := outbox.NewRepository(pool, "outbox_messages")
    
    // 1. Setup Notification Listener for reactive processing
    nl := outbox.NewNotificationListener(
        pool.Config().ConnConfig,
        "outbox_events", // Channel name used in Postgres trigger
        30*time.Second,  // Wait timeout
        3*time.Second,   // Close timeout
        outbox.NewRetryPolicy(
            outbox.NewBackoff(time.Second, 30*time.Second, 2),
            3,
        ),
    )

    // 2. Setup Provider (Hybrid: Notify + Poll)
    provider := outbox.NewProvider(
        repo,
        nl,
        5*time.Second, // Fallback polling interval
        100,           // Batch limit
    )

    // 3. Setup Dispatcher and Processor
    publisher := &MyKafkaPublisher{} 
    router := outbox.NewRouter(map[string]outbox.RouteResolver{
        outbox.RouteName("Order", "OrderCreated"): func(msg *outbox.Message) (outbox.Route, error) {
            return newMyRoute("order-topic", msg.AggregateID), nil
        },
    })
    dispatcher := outbox.NewDispatcher(publisher, router)
    processor := outbox.NewDefaultProcessor(dispatcher, repo.(outbox.Acknowledger))

    // 4. Start the worker
    worker := outbox.NewWorker(provider, processor)
    if err := worker.Run(ctx); err != nil {
        log.Fatalf("Worker failed: %v", err)
    }
}
```

## Configuration

- **Writer**: Used in your application code to insert messages.
- **Repository**: Handles fetching and updating message status in the database. Also acts as an `Acknowledger`.
- **NotificationListener**: Listens for PostgreSQL notifications to trigger immediate processing.
- **Provider**: Coordinates between notifications and polling to provide a stream of messages.
- **Processor**: Handles the processing logic for individual messages, including dispatching and acknowledgment.
- **Publisher**: You must provide an implementation of the `Publisher` interface (e.g., for Kafka, RabbitMQ, or SNS).
- **Worker**: Orchestrates the whole pipeline by consuming from the Provider and passing to the Processor.

## Error Handling & Retries

The worker will stop and return an error if a non-retryable error occurs or if the provider's error channel receives an error. Transient errors during processing are handled by the processor's interaction with the acknowledger.

## TODO

- [x] Add examples
- [ ] Add filter by aggregate type
- [ ] Add WAL support

## License

This project is licensed under the [LICENSE](LICENSE) file.
