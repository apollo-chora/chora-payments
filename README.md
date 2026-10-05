# chora-payments

Payments service for Chora: the canonical Stripe-correlation home for the
platform. It owns the Stripe SDK import, the inbound Stripe webhook receive
path, the synchronous gRPC `PaymentService` (Checkout Session + refund), and
the outbox-published event taxonomy on the NATS JetStream event bus.

The service is cloud-neutral: PostgreSQL for persistence, NATS JetStream for
events, env-backed configuration for secrets. No cloud account or managed
services (managed SQL, Pub/Sub, Secret Manager, or CI/CD) are required.

## What it does

1. **Stripe SDK** (`stripe-go/v78`) — exactly one import across the platform.
2. **Inbound Stripe HTTPS webhook** at `POST /v1/webhooks/stripe` —
   `Stripe-Signature` HMAC verify + dedup via `chora_payments.stripe_webhook_events`
   + dispatch to the matching Purchase aggregate.
3. **gRPC `PaymentService`** for synchronous Stripe Checkout Session + refund
   operations called by originating services.
4. **Outbox-published event taxonomy** — `chora.payments.{aggregate}.{event_type}.v1`
   — 28 SSE-visible subjects (7 Purchase aggregates × 4 state events) plus
   dispute and subscription-lifecycle events, drained from
   `chora_payments.outbox_events` to the event bus by the outbox dispatcher.

## Purchase aggregates

| Aggregate | Source flow |
|---|---|
| `course_purchase` | Paid course Stripe Checkout |
| `application_payment` | Course application payment |
| `familiar_egg_purchase` | Familiar/Companion egg buy |
| `tenant_mana_topup` | Tenant mana pool top-up |
| `user_subscription` | Per-learner Familiar mana subscription |
| `user_mana_topup` | Per-learner mana top-up |
| `identity_kyc_fee` | Identity KYC verification fee |
| `tenant_addon_purchase` | H+ Marketplace addon subscription |

Each aggregate carries the same 5-state payment FSM:
`checkout_started → payment_captured | payment_failed → refunded | expired`.

## Architecture

- **Compute**: any host running the Go binary or the container image.
- **Database**: PostgreSQL (`chora_payments`). Schema changes live in
  `migrations/` and are applied with the shared migration runner.
- **Event bus**: NATS JetStream (stream `CHORA_EVENTS`, DLQ stream
  `CHORA_DLQ`). The outbox dispatcher drains pending rows; the SSE fan-out
  broker subscribes back to the published subjects.
- **Ports**: HTTP `:8080` (Stripe webhook + `/healthz` + `/readyz` + admin
  `/api/v1/payments/*`); gRPC `:9090` (`PaymentService`).

## Configuration

Copy the example environment file:

```sh
cp .env.example .env
```

Important variables:

| Variable | Purpose | Local default |
| --- | --- | --- |
| `PORT` | HTTP port | `8080` |
| `CHORA_GRPC_PORT` | gRPC port (`GRPC_PORT` accepted as alias) | `9090` |
| `CHORA_DB_DSN` | PostgreSQL connection string (app_rw role) | Compose PostgreSQL |
| `NATS_URL` | NATS JetStream event bus | `nats://nats:4222` |
| `CHORA_SOURCE_PROJECT` | Project label stamped into event envelopes | `chora-local` |
| `STRIPE_API_KEY` | Stripe secret key (unset → stub adapter) | unset |
| `STRIPE_WEBHOOK_SECRET` | Stripe webhook signing secret | unset |
| `STRIPE_PRICE_CATALOGUE_SECRET_ID` | Env-backed secret with the Price ID catalogue JSON | unset |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP/gRPC trace endpoint | `http://otel-collector:4317` |

## Run locally

```sh
go run ./cmd/server
```

With `CHORA_DB_DSN` and `NATS_URL` set, the service wires the pg repos, the
outbox dispatcher, and the SSE fan-out broker. Unset, it runs on in-memory
adapters (not durable).

## Database migrations

Forward migrations are every `migrations/*.sql` except `*.down.sql`. Apply
them with the shared runner from `chora-stack/scripts/migrate.sh` (mount the
repo's `migrations/` at `/migrations` and set `CHORA_MIGRATE_DSN`).

## Tests

```sh
go test ./...                 # unit tests (hermetic)
go test -tags integration ./internal/adapter/repo/pg/  # real-Postgres tests
```

The integration tests skip unless `CHORA_TEST_DSN` is set.
