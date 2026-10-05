// webhook_event.go — Postgres adapter for the global Stripe webhook
// dedup table.
//
// SCHEMA: migrations/0001_initial.up.sql (`stripe_webhook_events`).
//
// RLS-DISABLED rationale (per migration): Stripe events arrive without
// tenant scope; tenant_id is in the Session metadata, resolved by the
// dispatch path AFTER the dedup gate. No rls.ApplySession here.
package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

const SQLInsertWebhookEvent = `
INSERT INTO stripe_webhook_events (event_id, event_type, received_at)
VALUES ($1, $2, $3)
`

const SQLUpdateWebhookEventProcessed = `
UPDATE stripe_webhook_events
   SET processed_at = $2,
       target_aggregate_type = $3,
       target_purchase_id = $4
 WHERE event_id = $1
`

const SQLUpdateWebhookEventFailed = `
UPDATE stripe_webhook_events
   SET processing_error = $2
 WHERE event_id = $1
`

const SQLSelectWebhookEventByID = `
SELECT event_id, event_type, received_at, processed_at, processing_error,
       target_aggregate_type, target_purchase_id
FROM stripe_webhook_events
WHERE event_id = $1
`

type WebhookEventRepo struct {
	tx TxRunner
}

func NewWebhookEventRepo(tx TxRunner) *WebhookEventRepo {
	return &WebhookEventRepo{tx: tx}
}

// Insert is the canonical Stripe dedup gate. UNIQUE on event_id; on
// conflict returns wh.ErrAlreadyProcessed so the receive path can
// short-circuit to 200 OK + ack without re-processing.
func (r *WebhookEventRepo) Insert(ctx context.Context, w *wh.WebhookEvent) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if w == nil {
		return wh.ErrEventIDRequired
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		_, err := q.Exec(ctx, SQLInsertWebhookEvent, w.EventID, w.EventType, w.ReceivedAt)
		if err != nil {
			if isUniqueViolation(err) {
				return wh.ErrAlreadyProcessed
			}
			return fmt.Errorf("pg: insert stripe_webhook_event: %w", err)
		}
		return nil
	})
}

func (r *WebhookEventRepo) MarkProcessed(ctx context.Context, eventID string, agg wh.AggregateType, purchaseID string, processedAt time.Time) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		_, err := q.Exec(ctx, SQLUpdateWebhookEventProcessed,
			eventID, processedAt, string(agg), nullStr(purchaseID),
		)
		if err != nil {
			return fmt.Errorf("pg: mark webhook_event processed: %w", err)
		}
		return nil
	})
}

func (r *WebhookEventRepo) MarkFailed(ctx context.Context, eventID, processingError string) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		_, err := q.Exec(ctx, SQLUpdateWebhookEventFailed, eventID, processingError)
		if err != nil {
			return fmt.Errorf("pg: mark webhook_event failed: %w", err)
		}
		return nil
	})
}

func (r *WebhookEventRepo) GetByEventID(ctx context.Context, eventID string) (*wh.WebhookEvent, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if eventID == "" {
		return nil, wh.ErrEventIDRequired
	}
	var found *wh.WebhookEvent
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		row := q.QueryRow(ctx, SQLSelectWebhookEventByID, eventID)
		w, scanErr := scanWebhookEvent(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return shared.ErrNotFound
			}
			return scanErr
		}
		found = w
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

var _ wh.Repo = (*WebhookEventRepo)(nil)

func scanWebhookEvent(scan func(...any) error) (*wh.WebhookEvent, error) {
	var (
		eventID, eventType string
		receivedAt         time.Time
		processedAt        *time.Time
		processingError    *string
		targetAggregate    *string
		targetPurchaseID   *string
	)
	if err := scan(
		&eventID, &eventType, &receivedAt, &processedAt,
		&processingError, &targetAggregate, &targetPurchaseID,
	); err != nil {
		if isNoRows(err) {
			return nil, errScanNotFound
		}
		return nil, err
	}
	w := &wh.WebhookEvent{
		EventID:     eventID,
		EventType:   eventType,
		ReceivedAt:  receivedAt.UTC(),
		ProcessedAt: timeUTC(processedAt),
	}
	if processingError != nil {
		w.ProcessingError = *processingError
	}
	if targetAggregate != nil {
		w.TargetAggregateType = wh.AggregateType(*targetAggregate)
	}
	if targetPurchaseID != nil {
		w.TargetPurchaseID = *targetPurchaseID
	}
	return w, nil
}

// isUniqueViolation matches Postgres SQLSTATE 23505 loosely (string
// match) so this package binds neither pq nor pgx at compile time.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "23505") || strings.Contains(msg, "duplicate key value")
}
