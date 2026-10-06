# chora-payments

## About

chora-payments is a Go service that owns payment operations for Chora. It is the platform's Stripe integration point for Checkout Sessions, refunds, subscription changes, Customer Portal sessions, and Stripe webhook processing. It persists purchase state in PostgreSQL, publishes payment events through a transactional outbox to NATS JetStream, and exposes both gRPC and HTTP interfaces.

## Quick start

### Prerequisites

- Go 1.26.1 or newer
- PostgreSQL, when using durable persistence
- NATS JetStream, when using the event bus and SSE stream
- A Stripe API key for real Stripe calls. Without one, the service uses its local stub adapter.

### Run the service

From the repository root:

```sh
cp .env.example .env
go run ./cmd/server
```

The server listens on HTTP `:8080` and gRPC `:9090` by default.

For a local process without PostgreSQL or NATS, leave `CHORA_DB_DSN` and `NATS_URL` unset. The service then uses in-memory repositories and a no-op outbox emitter, so data is not durable across restarts and the SSE stream is unavailable.

To use PostgreSQL and NATS, set `CHORA_DB_DSN` and `NATS_URL` in the environment before starting the service. Apply the SQL files under `migrations/` before using the PostgreSQL-backed repositories.

## Usage

### HTTP

Health endpoints:

```text
GET /healthz
GET /readyz
```

Both return JSON with the service status, version, and build Git SHA.

Stripe webhook ingress:

```text
POST /v1/webhooks/stripe
```

The handler requires `STRIPE_WEBHOOK_SECRET` and validates the `Stripe-Signature` header before deduplicating and dispatching the event. Failed dispatches return a non-2xx response so Stripe can retry the delivery.

Legacy admin payment endpoints:

```text
GET  /api/v1/payments/{purchase_id}?aggregate_type=...&tenant_id=...
POST /api/v1/payments/{purchase_id}/refund
```

These endpoints require an allowed `X-Chora-Role` header. Tenant-scoped requests can supply the tenant through `X-Chora-Tenant-Id`, `X-Tenant-Id`, `chora-tenant-id`, or `tenant_id` in the query string/body.

Transaction history and payment-event endpoints:

```text
GET  /api/v1/admin/payments/purchases
POST /api/v1/admin/payments/{purchase_id}/refund
GET  /api/v1/admin/payments/export?format=csv|json
GET  /api/v1/admin/payments/stream
```

The canonical admin roles are `TENANT_ADMIN`, `OWNER`, `AUDITOR`, and `PLATFORM_OPERATOR`. `AUDITOR` is read-only. `PLATFORM_OPERATOR` can query across tenants; other canonical roles are tenant-scoped.

Purchase-list filters include `aggregate_type`, `state`, `from`, `to`, `page_size`, `page_token`, and, for `PLATFORM_OPERATOR`, `tenant_id`.

Tenant billing endpoints:

```text
GET  /api/v1/admin/tenants/{tenantId}/invoices?limit=&starting_after=
POST /api/v1/admin/tenants/{tenantId}/billing-portal
```

The invoice endpoint returns paginated Stripe invoices for the tenant's Stripe Customer. The billing-portal endpoint accepts a JSON body containing `return_url` and returns the one-time Stripe Customer Portal URL.

H+ Marketplace subscription endpoints:

```text
POST /api/v1/admin/tenant-addons:change-tier
POST /api/v1/admin/tenant-addons:preview-tier-change
```

Both accept `tenant_id`, `addon_code`, `target_tier_code`, and `currency`. Tier changes also support `proration_mode` and `effective_at`; `effective_at=end_of_cycle` schedules the change for the next billing cycle.

### gRPC

The service implements `chora.services.payments.v1.PaymentService` on port `:9090`, including Checkout Session creation, purchase lookup, and refunds. The generated contract is provided by `github.com/apollo-chora/chora-contracts/gen/go`.

The service also registers the standard gRPC health service and reports `chora.payments.v1.PaymentService` as serving.

### Configuration

Configuration is environment-based. The example file is `.env.example`.

Key variables:

| Variable | Purpose |
| --- | --- |
| `PORT` | HTTP listen port. Default: `8080`. |
| `CHORA_GRPC_PORT` | gRPC listen port. Default: `9090`. `GRPC_PORT` is accepted as an alias. |
| `CHORA_DB_DSN` | PostgreSQL DSN for durable repositories. |
| `CHORA_DB_DSN_SECRET_ID` | Secret identifier used to resolve the PostgreSQL DSN. |
| `CHORA_DB_PROJECT` | Project used for secret resolution. Defaults to `chora-489812`. |
| `NATS_URL` | NATS JetStream URL. |
| `STRIPE_API_KEY` | Stripe API key. Unset selects the local stub adapter. |
| `STRIPE_TEST_MODE` | Stripe test-mode flag passed to the Stripe adapter. |
| `STRIPE_WEBHOOK_SECRET` | Stripe webhook signing secret. |
| `STRIPE_PRICE_CATALOGUE_SECRET_ID` | Secret containing the Stripe Price catalogue JSON. |
| `STRIPE_DEFAULT_SUCCESS_URL` | Default Checkout success URL. |
| `STRIPE_DEFAULT_CANCEL_URL` | Default Checkout cancel URL. |
| `CHORA_SOURCE_PROJECT` | Source-project label written into event envelopes. |
| `CHORA_OUTBOX_WORKER_ID` | Outbox worker identifier. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP/gRPC tracing endpoint. |

## Development

The module path is `github.com/apollo-chora/chora-payments`. The executable entry point is `cmd/server`.

The main code is split into:

- `internal/domain/`: purchase, subscription, dispute, webhook, and shared domain logic.
- `internal/adapter/grpc/`: gRPC `PaymentService` implementation.
- `internal/adapter/http/`: webhook, admin, billing, tier-change, and health-related HTTP handlers.
- `internal/adapter/stripe/`: the Stripe SDK adapter and real/stub clients.
- `internal/adapter/stripe_catalogue/`: Stripe Price ID catalogue loading and lookup.
- `internal/adapter/repo/pg/`: PostgreSQL repositories.
- `internal/adapter/repo/inmem/`: in-memory repositories used when no database is configured.
- `internal/adapter/outbox/`: transactional outbox emission and dispatch.
- `internal/adapter/pubsub/event_broker/`: NATS-backed payment-event fan-out for SSE.
- `migrations/`: PostgreSQL schema migrations.

Run the unit test suite with:

```sh
go test ./...
```

Run the PostgreSQL integration tests with:

```sh
go test -tags integration ./internal/adapter/repo/pg/
```

Those integration tests require `CHORA_TEST_DSN`; without it they skip.

Build the server binary with:

```sh
go build ./cmd/server
```

Build the container image with the repository's Dockerfile:

```sh
docker build -t chora-payments .
```

