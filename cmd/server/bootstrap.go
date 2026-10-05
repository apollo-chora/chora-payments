// bootstrap.go — production wiring helpers for chora-payments.
//
// Per `feedback_resilience_priority` + secrets-and-env: production
// dependencies come from env vars. Local dev sees nil pools / nil bus
// clients so the in-memory adapter fallback works.
//
// Environment contract:
//
//	CHORA_DB_DSN_SECRET_ID  — env-backed secret name resolving to
//	                          a chora_payments DSN (app_rw role).
//	CHORA_DB_DSN            — direct DSN (dev override).
//	CHORA_DB_PROJECT        — project label used for secret resolution.
//	                          Defaults to chora-local.
//	CHORA_DB_REWRITE_FROM_PORT / CHORA_DB_REWRITE_TO_PORT
//	                        — PgBouncer 6432 -> Postgres 5432 rewrite
//	                          for build-out phase.
//
//	NATS_URL                — NATS JetStream broker URL for the event bus.
//
// Empty env → fall through to in-memory adapters / NoOpEmitter.
package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	cgcdb "github.com/apollo-chora/chora-common/db"
	"github.com/apollo-chora/chora-common/eventbus"
	cgcsecrets "github.com/apollo-chora/chora-common/secrets"

	"github.com/apollo-chora/chora-payments/internal/adapter/stripe_catalogue"
)

// bootstrapDBPool returns a pgxpool.Pool for chora_payments when env is
// configured; nil otherwise.
func bootstrapDBPool(ctx context.Context) (*pgxpool.Pool, func()) {
	dsn := os.Getenv("CHORA_DB_DSN")
	secretID := os.Getenv("CHORA_DB_DSN_SECRET_ID")
	if dsn == "" && secretID == "" {
		log.Printf("chora-payments: CHORA_DB_DSN / CHORA_DB_DSN_SECRET_ID unset — using in-memory repositories")
		return nil, nil
	}

	project := os.Getenv("CHORA_DB_PROJECT")
	if project == "" {
		project = "chora-489812"
	}

	var fetcher cgcdb.SecretFetcher
	var sclient *cgcsecrets.Client
	if secretID != "" && dsn == "" {
		c, err := cgcsecrets.NewClient(ctx, project)
		if err != nil {
			log.Fatalf("chora-payments: secret resolver init failed (env set, fail-loud): %v", err)
		}
		sclient = c
		fetcher = c
	}

	rewriteFrom, _ := strconv.Atoi(os.Getenv("CHORA_DB_REWRITE_FROM_PORT"))
	rewriteTo, _ := strconv.Atoi(os.Getenv("CHORA_DB_REWRITE_TO_PORT"))

	bootstrapSecs, _ := strconv.Atoi(os.Getenv("CHORA_BOOTSTRAP_TIMEOUT_SECONDS"))
	if bootstrapSecs <= 0 {
		bootstrapSecs = 30
	}
	bootstrapCtx, cancel := context.WithTimeout(ctx, time.Duration(bootstrapSecs)*time.Second)
	defer cancel()

	pool, err := cgcdb.Bootstrap(bootstrapCtx, cgcdb.BootstrapOptions{
		DSN:             dsn,
		SecretID:        secretID,
		SecretFetcher:   fetcher,
		RewriteFromPort: rewriteFrom,
		RewriteToPort:   rewriteTo,
		AppName:         serviceName + "@" + serviceVersion,
		RuntimeParams:   paymentsDBRuntimeParams(),
	})
	if err != nil {
		if sclient != nil {
			_ = sclient.Close()
		}
		log.Fatalf("chora-payments: pgx pool bootstrap failed (env set, fail-loud): %v", err)
	}

	shutdown := func() {
		pool.Close()
		if sclient != nil {
			_ = sclient.Close()
		}
	}
	return pool, shutdown
}

// bootstrapBus returns a NATS JetStream eventbus when NATS_URL is set,
// otherwise nil (the caller falls back to the in-memory bus). The returned
// closure is the shutdown hook; caller defers it.
//
// Environment contract:
//
//	NATS_URL — JetStream broker URL (e.g. nats://127.0.0.1:4222).
//	           Unset → nil bus (in-process fallback; NOT durable).
func bootstrapBus(ctx context.Context) (eventbus.Bus, func()) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		log.Printf("chora-payments: NATS_URL unset — outbox emitter degrades to NoOpEmitter")
		return nil, nil
	}
	bus, err := eventbus.NewJetStream(eventbus.JetStreamConfig{URL: url})
	if err != nil {
		log.Printf("chora-payments: NATS JetStream init failed: %v — falling back to NoOpEmitter", err)
		return nil, nil
	}
	return bus, func() { _ = bus.Close() }
}

// consumerConfig is the shared durable-consumer tuning for every
// chora-payments subscriber: at-least-once with a 30s ack window, five
// delivery attempts, and the canonical _dlq.<subject> dead-letter routing.
//
// The dotted event bus subscription id is safe as Name — eventbus sanitises it
// to a NATS-legal durable name internally.
func consumerConfig(name, subject string) eventbus.ConsumerConfig {
	return eventbus.ConsumerConfig{
		Name:       name,
		Subject:    subject,
		MaxDeliver: 5,
		AckWait:    30 * time.Second,
		Backoff: []time.Duration{
			1 * time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second,
		},
		DLQSubject: eventbus.DLQSubject(subject),
	}
}

// bootstrapPriceCatalogue resolves the chora-payments Stripe Price ID
// catalogue per CHO-1760. Env contract:
//
//	STRIPE_PRICE_CATALOGUE_SECRET_ID   — env-backed secret name holding
//	                                     the JSON blob.
//	CHORA_DB_PROJECT                   — project label (reused; same
//	                                     project hosts the secret).
//
// Unset env → PlaceholderPriceCatalogue (CHO-1762 + CHO-1764 refuse to
// call Stripe with synthetic `price_TODO_` IDs, so this is safe for
// unit tests + fresh dev boxes but fails-loud on a real Subscribe).
//
// Set env + secret unreachable → log.Fatalf (fail-loud per
// secrets-and-env policy).
func bootstrapPriceCatalogue(ctx context.Context) (stripe_catalogue.PriceCatalogue, func()) {
	secretID := os.Getenv("STRIPE_PRICE_CATALOGUE_SECRET_ID")
	if secretID == "" {
		log.Printf("chora-payments: WARNING: STRIPE_PRICE_CATALOGUE_SECRET_ID unset — Subscribe + change-tier will refuse on placeholder Price IDs (CHO-1760)")
		return stripe_catalogue.NewPlaceholderPriceCatalogue(), nil
	}

	project := os.Getenv("CHORA_DB_PROJECT")
	if project == "" {
		project = "chora-489812"
	}

	sclient, err := cgcsecrets.NewClient(ctx, project)
	if err != nil {
		log.Fatalf("chora-payments: STRIPE_PRICE_CATALOGUE_SECRET_ID set but secret resolver init failed (fail-loud): %v", err)
	}

	bootstrapSecs, _ := strconv.Atoi(os.Getenv("CHORA_BOOTSTRAP_TIMEOUT_SECONDS"))
	if bootstrapSecs <= 0 {
		bootstrapSecs = 30
	}
	bootstrapCtx, cancel := context.WithTimeout(ctx, time.Duration(bootstrapSecs)*time.Second)
	defer cancel()

	cat, err := stripe_catalogue.NewEnvPriceCatalogue(bootstrapCtx, sclient, secretID)
	if err != nil {
		_ = sclient.Close()
		log.Fatalf("chora-payments: Stripe Price catalogue load failed (fail-loud): %v", err)
	}
	log.Printf("chora-payments: Stripe Price catalogue loaded from env-backed secret %q", secretID)

	shutdown := func() {
		_ = sclient.Close()
	}
	return cat, shutdown
}

func outboxWorkerID() string {
	if v := os.Getenv("CHORA_OUTBOX_WORKER_ID"); v != "" {
		return v
	}
	if v := os.Getenv("HOSTNAME"); v != "" {
		return v
	}
	return "chora-payments-local"
}

// paymentsDBRuntimeParams returns the per-connection Postgres GUCs that keep a
// DB write from hanging forever (fail-loud). Platform-wide rollout of the
// CHO-2005 fix (2026-07-04); mirrors chora-consumption's
// consumptionDBRuntimeParams. Set on every pooled connection via
// BootstrapOptions.RuntimeParams.
//
//   - lock_timeout=3s: a statement blocked on a row lock ERRORs ("canceling
//     statement due to lock timeout") instead of waiting indefinitely and
//     leaking the request goroutine — under the gateway's 6s per-call
//     timeout, so this service fails loud (500) before the gateway 504s.
//   - idle_in_transaction_session_timeout=60s: reaps a leaked open
//     transaction so its row locks release.
//
// No statement_timeout (owner steer): long read paths must not be capped.
func paymentsDBRuntimeParams() map[string]string {
	return map[string]string{
		"lock_timeout":                        "3s",
		"idle_in_transaction_session_timeout": "60s",
	}
}
