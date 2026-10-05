// stripe_customer.go — Postgres adapter for the StripeCustomer registry.
//
// SCHEMA: migrations/0004_stripe_customers.up.sql.
//
// RLS-aware: tenant_isolation policy filters by chora.tenant_id; caller
// MUST set tenant_id on ctx via tracing.WithTenantID.
package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/rls"
	stripecustomer "github.com/apollo-chora/chora-payments/internal/domain/stripe_customer"
)

const SQLInsertStripeCustomer = `
INSERT INTO stripe_customers (tenant_id, learner_gcid, stripe_customer_id, email, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, learner_gcid) DO NOTHING
`

const SQLSelectStripeCustomerByGCID = `
SELECT tenant_id, learner_gcid, stripe_customer_id, email, created_at, updated_at
FROM stripe_customers
WHERE tenant_id = $1
  AND learner_gcid = $2
`

type StripeCustomerRepo struct {
	tx TxRunner
}

func NewStripeCustomerRepo(tx TxRunner) *StripeCustomerRepo {
	return &StripeCustomerRepo{tx: tx}
}

func (r *StripeCustomerRepo) GetByGCID(ctx context.Context, tenantID, learnerGCID string) (*stripecustomer.StripeCustomer, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if tenantID == "" {
		return nil, stripecustomer.ErrTenantRequired
	}
	if learnerGCID == "" {
		return nil, stripecustomer.ErrLearnerRequired
	}
	var found *stripecustomer.StripeCustomer
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		row := q.QueryRow(ctx, SQLSelectStripeCustomerByGCID, tenantID, learnerGCID)
		sc, scanErr := scanStripeCustomer(row.Scan)
		if scanErr != nil {
			if errors.Is(scanErr, errScanNotFound) {
				return stripecustomer.ErrNotFound
			}
			return scanErr
		}
		found = sc
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

func (r *StripeCustomerRepo) Insert(ctx context.Context, sc *stripecustomer.StripeCustomer) error {
	if r == nil || r.tx == nil {
		return ErrNotImplemented
	}
	if sc == nil {
		return stripecustomer.ErrStripeCustomerEmpty
	}
	return r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		_, err := q.Exec(ctx, SQLInsertStripeCustomer,
			sc.TenantID, sc.LearnerGCID, sc.StripeCustomerID, nullStr(sc.Email),
			sc.CreatedAt, sc.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("pg: insert stripe_customer: %w", err)
		}
		return nil
	})
}

var _ stripecustomer.Repo = (*StripeCustomerRepo)(nil)

// SQLSelectStripeCustomersByGCIDs hydrates email + customer-id batch-wise.
// Used by purchase_history.go to avoid an N+1 GetByGCID across the
// UNION ALL result.
//
// The tenant_isolation RLS policy on stripe_customers narrows to the
// caller's SET LOCAL chora.tenant_id; the explicit tenant_id = $1
// predicate is defense-in-depth + lets the planner pick the
// (tenant_id, learner_gcid) index.
const SQLSelectStripeCustomersByGCIDs = `
SELECT learner_gcid, email
FROM stripe_customers
WHERE tenant_id = $1
  AND learner_gcid = ANY($2::uuid[])
`

// GetByGCIDs returns a map[learner_gcid]→StripeCustomer for the supplied
// GCID set. Used by purchase_history.go for email hydration; avoids an
// N+1 GetByGCID per row.
//
// Empty or whitespace-only entries are silently dropped; duplicates are
// deduplicated; an empty input slice returns an empty map without any
// DB call.
func (r *StripeCustomerRepo) GetByGCIDs(ctx context.Context, tenantID string, gcids []string) (map[string]*stripecustomer.StripeCustomer, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	cleaned := dedupeGCIDs(gcids)
	out := make(map[string]*stripecustomer.StripeCustomer, len(cleaned))
	if len(cleaned) == 0 {
		return out, nil
	}
	if tenantID == "" {
		return nil, stripecustomer.ErrTenantRequired
	}
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if err := rls.ApplySession(ctx, qToExecer(q)); err != nil {
			return err
		}
		rows, err := q.Query(ctx, SQLSelectStripeCustomersByGCIDs, tenantID, cleaned)
		if err != nil {
			return fmt.Errorf("pg: query stripe_customers batch: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var (
				gcid  string
				email *string
			)
			if scanErr := rows.Scan(&gcid, &email); scanErr != nil {
				return fmt.Errorf("pg: scan stripe_customers batch row: %w", scanErr)
			}
			sc := &stripecustomer.StripeCustomer{
				TenantID:    tenantID,
				LearnerGCID: gcid,
			}
			if email != nil {
				sc.Email = *email
			}
			out[gcid] = sc
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// dedupeGCIDs drops empty/whitespace entries + duplicates. Order is
// preserved (first occurrence wins) so SQL test assertions are
// deterministic.
func dedupeGCIDs(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func scanStripeCustomer(scan func(...any) error) (*stripecustomer.StripeCustomer, error) {
	var (
		tenantID, learnerGCID, stripeCustomerID string
		email                                   *string
		createdAt, updatedAt                    time.Time
	)
	if err := scan(&tenantID, &learnerGCID, &stripeCustomerID, &email, &createdAt, &updatedAt); err != nil {
		if isNoRows(err) {
			return nil, errScanNotFound
		}
		return nil, err
	}
	sc := &stripecustomer.StripeCustomer{
		TenantID:         tenantID,
		LearnerGCID:      learnerGCID,
		StripeCustomerID: stripeCustomerID,
		CreatedAt:        createdAt.UTC(),
		UpdatedAt:        updatedAt.UTC(),
	}
	if email != nil {
		sc.Email = *email
	}
	return sc, nil
}
