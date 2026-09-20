package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	outbox "github.com/ivan-prykhodko/pgx-outbox"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pgxClient, err := newPgxClient(ctx)
	if err != nil {
		log.Println(err)
		return
	}

	done := make(chan os.Signal, 1)

	worker := newWorker(pgxClient)
	var workerErr chan error
	go func() {
		if wErr := worker.Run(ctx); wErr != nil {
			workerErr <- wErr
		}
	}()

	writer := newWriter()
	err = populate(ctx, pgxClient, writer)
	if err != nil {
		log.Println(err)
		return
	}

	select {
	case err := <-workerErr:
		if err != nil {
			log.Println(err)
		}
	case <-done:
	}

	fmt.Println("Bye!")
}

const TableName = "outbox_messages"

func newPgxClient(ctx context.Context) (*pgxpool.Pool, error) {
	url := "postgres://db_user:db_password@app_database:5432/db_name?search_path=public&sslmode=disable&TimeZone=UTC"

	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	//logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	//config.ConnConfig.Tracer = &tracelog.TraceLog{
	//	Logger: tracelog.LoggerFunc(func(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]any) {
	//		logger.Log(ctx, slog.LevelInfo, msg, "level", level.String(), "data", data)
	//	}),
	//	LogLevel: tracelog.LogLevelInfo,
	//}

	return pgxpool.NewWithConfig(ctx, config)
}

// Worker services
type route struct {
	data map[string]any
}

func newRoute(topic string, key string, idempotencyKey string) outbox.Route {
	return &route{
		data: map[string]any{
			"topic":           topic,
			"key":             key,
			"idempotency_key": idempotencyKey,
		},
	}
}

func (r *route) Data() map[string]any {
	return r.data
}

type outputPublisher struct{}

func (o *outputPublisher) Publish(ctx context.Context, env *outbox.Envelope) error {
	fmt.Printf("publish: %v\n", *env)
	return nil
}

func newRepository(pool *pgxpool.Pool) outbox.Repository {
	return outbox.NewRepository(pool, TableName)
}

func newProcessor(acknowledger outbox.Acknowledger) outbox.Processor {
	orderRouteResolver := func(msg *outbox.Message) (outbox.Route, error) {
		return newRoute(
			"order",
			"",
			fmt.Sprintf("outbox:%s:%s:%s:%d", msg.AggregateType, msg.AggregateID, msg.EventType, msg.ID),
		), nil
	}
	router := outbox.NewRouter(map[string]outbox.RouteResolver{
		outbox.RouteName("Order", "order.created"):   orderRouteResolver,
		outbox.RouteName("Order", "order.confirmed"): orderRouteResolver,
	})
	publisher := &outputPublisher{}
	dispatcher := outbox.NewDispatcher(publisher, router)

	return outbox.NewDefaultProcessor(dispatcher, acknowledger)
}

func newWorker(pool *pgxpool.Pool) outbox.Worker {
	poolConfig := pool.Config()

	repo := newRepository(pool)

	nl := outbox.NewNotificationListener(
		poolConfig.ConnConfig,
		"outbox_events",
		30*time.Second,
		3*time.Second,
		outbox.NewRetryPolicy(
			outbox.NewBackoff(time.Second, 30*time.Second, 2),
			3,
		),
	)

	provider := outbox.NewProvider(repo, nl, 5*time.Second, 5)

	processor := newProcessor(repo.(outbox.Acknowledger))

	return outbox.NewWorker(provider, processor)
}

// Population data services
func newWriter() outbox.Writer {
	return outbox.NewWriter(TableName)
}

type orderCreatedEvent struct {
	ID         int64
	OrderID    string
	OccurredAt string
	Name       string
}

func createMessage(event orderCreatedEvent) outbox.Message {
	payload, _ := json.Marshal(event)
	occurredAt, _ := time.Parse(time.RFC3339, event.OccurredAt)
	return outbox.NewMessage(
		"Order",
		event.OrderID,
		event.Name,
		payload,
		map[string]string{},
		occurredAt,
	)
}

var eventId atomic.Int64

func populateSome(ctx context.Context, pool *pgxpool.Pool, writer outbox.Writer) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		msg := createMessage(orderCreatedEvent{
			eventId.Load(),
			"100501",
			time.Now().UTC().Format(time.RFC3339Nano),
			"order.created",
		})
		eventId.Add(1)

		_, err := writer.Write(ctx, tx, &msg)
		if err != nil {
			return err
		}

		return nil
	})
}

func populate(ctx context.Context, pool *pgxpool.Pool, writer outbox.Writer) error {
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			err := populateSome(ctx, pool, writer)
			if err != nil {
				return err
			}
		}
	}
}
