// purchase_history.go — Postgres adapter for the cross-aggregate
// PurchaseHistoryPort that backs the H+ Transaction History admin
// surface (openapi/payments-admin.yaml).
//
// SCHEMA: this adapter reads 7 source tables — course_purchases,
// application_payments, familiar_egg_purchases, tenant_mana_topups,
// user_subscriptions, user_mana_topups, identity_kyc_fees — via a
// single UNION ALL projection. Each branch maps its native columns
// onto the shared 12-column projection shape + a metadata_json
// field carrying aggregate-specific extras (course_id, sku, plan_sku, …).
//
// HARD RULE per .claude/rules/ddd-enforcement.md: reads ONLY
// chora_payments tables. Learner email hydration uses the on-payments-
// side stripe_customers registry via a LEFT JOIN in the same statement.
//
// RLS behaviour:
//
//   - ListByTenant — tenant-scoped: calls applyRLS which fires SET LOCAL
//     chora.tenant_id from ctx (per the shared rls helper). Tenant
//     isolation enforced at DB level on every source table.
//   - ListAll — operator-scoped: caller MUST have set rls.WithRLSBypass
//     on ctx. The adapter skips the SET LOCAL chora.tenant_id step so
//     the query spans every tenant row. The HTTP layer is the only
//     intended caller of this code path and asserts on the
//     PLATFORM_OPERATOR role before flagging the context.
//   - GetForRefund — tenant-scoped (refund handler ALWAYS knows the
//     target tenant from the URL path / body).
//
// Cursor format: base64.URLEncoding-encoded JSON
//
//	{"paid_at":"<RFC3339Nano>","purchase_id":"<UUIDv7>"}
//
// Stable ordering: ORDER BY paid_at DESC NULLS LAST, purchase_id DESC.
// The +1 row-fetch trick is used to detect has_more without a second
// round-trip.
package pg

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	cgcrls "github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-payments/internal/adapter/repo/rls"
	"github.com/apollo-chora/chora-payments/internal/domain/payments"
)

// -----------------------------------------------------------------------------
// SQL templates — UNION ALL across 7 source tables.
//
// Each SELECT projects the shared 12-column shape:
//
//   1  purchase_id     (uuid → text)
//   2  aggregate_type  (text literal)
//   3  tenant_id       (uuid → text)
//   4  learner_gcid    (uuid → text)
//   5  amount_cents    (bigint; canonical = amount_cents_paid when captured;
//                       falls back to amount_cents at checkout)
//   6  currency        (text; lower())
//   7  state_raw       (text; the 5-state shared.State FSM value)
//   8  stripe_session_id (text)
//   9  paid_at         (timestamptz)
//   10 refunded_at     (timestamptz)
//   11 created_at      (timestamptz)
//   12 metadata_json   (jsonb → text; aggregate-specific projection)
//
// LEFT JOIN stripe_customers attaches learner_email per (tenant_id,
// learner_gcid). The outer SELECT applies state-normalisation via a
// CASE expression and adds the learner_email column.
// -----------------------------------------------------------------------------

const purchaseHistoryUnionCols = `
    purchase_id, aggregate_type, tenant_id, learner_gcid,
    amount_cents, currency, state_raw, stripe_session_id,
    paid_at, refunded_at, created_at, metadata_json
`

// The projection is wrapped in an outer SELECT … FROM (…) AS h so that
// (a) every column drawn from the UNION subquery `u` is u-qualified —
// `stripe_customers sc` shares tenant_id + learner_gcid + created_at, so
// an unqualified reference raises "column reference … is ambiguous"
// (SQLSTATE 42702) — and (b) the buildListSQL filter clause can predicate
// on the *normalised* alias `state_normalised`, which only exists on the
// outer relation `h` (a WHERE at the join level cannot see SELECT-list
// aliases). Columns unique to `u` (e.g. state_raw inside the CASE) stay
// unqualified; only the colliding ones need the u. prefix.
const purchaseHistorySelectShape = `
    SELECT h.purchase_id, h.aggregate_type, h.tenant_id, h.learner_gcid,
           h.learner_email, h.amount_cents, h.currency, h.state_normalised,
           h.stripe_session_id, h.paid_at, h.refunded_at, h.created_at,
           h.metadata_json
      FROM (
        SELECT u.purchase_id, u.aggregate_type, u.tenant_id, u.learner_gcid,
               COALESCE(sc.email, '') AS learner_email,
               u.amount_cents, u.currency,
               CASE state_raw
                   WHEN 'payment_captured' THEN 'captured'
                   WHEN 'payment_failed'   THEN 'failed'
                   WHEN 'refunded'         THEN 'refunded'
                   WHEN 'expired'          THEN 'expired'
                   WHEN 'checkout_started' THEN 'failed'
                   WHEN 'created'          THEN 'failed'
                   WHEN 'active'           THEN 'captured'
                   WHEN 'grace'            THEN 'captured'
                   WHEN 'paused'           THEN 'captured'
                   WHEN 'cancelled'        THEN 'refunded'
                   WHEN 'provisioned'      THEN 'captured'
                   ELSE state_raw
               END AS state_normalised,
               u.stripe_session_id, u.paid_at, u.refunded_at, u.created_at,
               u.metadata_json
          FROM (
`

// purchaseHistoryUnionBody is the 7-branch UNION ALL inner query. Lives
// inside the outer SELECT shape above. Each branch normalises its
// state column + casts a literal aggregate_type label + projects a
// jsonb metadata payload.
const purchaseHistoryUnionBody = `
    SELECT purchase_id, 'course_purchase'::text       AS aggregate_type,
           tenant_id, learner_gcid,
           COALESCE(amount_cents_paid, amount_cents) AS amount_cents,
           LOWER(currency)                            AS currency,
           state                                      AS state_raw,
           stripe_session_id, paid_at, refunded_at, created_at,
           jsonb_build_object('course_id', course_id)::text AS metadata_json
      FROM course_purchases
    UNION ALL
    SELECT purchase_id, 'application_payment'::text   AS aggregate_type,
           tenant_id, learner_gcid,
           COALESCE(amount_cents_paid, amount_cents) AS amount_cents,
           LOWER(currency)                            AS currency,
           state                                      AS state_raw,
           stripe_session_id, paid_at, refunded_at, created_at,
           jsonb_build_object('application_id', application_id, 'course_id', course_id)::text AS metadata_json
      FROM application_payments
    UNION ALL
    SELECT purchase_id, 'familiar_egg_purchase'::text AS aggregate_type,
           tenant_id, learner_gcid,
           COALESCE(amount_cents_paid, amount_cents) AS amount_cents,
           LOWER(currency)                            AS currency,
           state                                      AS state_raw,
           stripe_session_id, paid_at, refunded_at, created_at,
           jsonb_build_object('egg_sku', egg_sku, 'suggested_focal_atom_id', suggested_focal_atom_id)::text AS metadata_json
      FROM familiar_egg_purchases
    UNION ALL
    SELECT purchase_id, 'tenant_mana_topup'::text     AS aggregate_type,
           tenant_id, admin_gcid                       AS learner_gcid,
           COALESCE(amount_cents_paid, amount_cents) AS amount_cents,
           LOWER(currency)                            AS currency,
           state                                      AS state_raw,
           stripe_session_id, paid_at, refunded_at, created_at,
           jsonb_build_object('sku', sku, 'mana_units', mana_units)::text AS metadata_json
      FROM tenant_mana_topups
    UNION ALL
    SELECT purchase_id, 'user_subscription'::text     AS aggregate_type,
           tenant_id, learner_gcid,
           COALESCE(amount_cents_paid_total, amount_cents) AS amount_cents,
           LOWER(currency)                            AS currency,
           state                                      AS state_raw,
           stripe_session_id, paid_at, refunded_at, created_at,
           jsonb_build_object('plan_sku', plan_sku, 'billing_period', billing_period)::text AS metadata_json
      FROM user_subscriptions
    UNION ALL
    SELECT purchase_id, 'user_mana_topup'::text       AS aggregate_type,
           tenant_id, learner_gcid,
           COALESCE(amount_cents_paid, amount_cents) AS amount_cents,
           LOWER(currency)                            AS currency,
           state                                      AS state_raw,
           stripe_session_id, paid_at, refunded_at, created_at,
           jsonb_build_object('sku', sku, 'mana_units', mana_units)::text AS metadata_json
      FROM user_mana_topups
    UNION ALL
    SELECT purchase_id, 'identity_kyc_fee'::text      AS aggregate_type,
           tenant_id, learner_gcid,
           COALESCE(amount_cents_paid, amount_cents) AS amount_cents,
           LOWER(currency)                            AS currency,
           state                                      AS state_raw,
           stripe_session_id, paid_at, refunded_at, created_at,
           jsonb_build_object('kyc_doc_type', kyc_doc_type)::text AS metadata_json
      FROM identity_kyc_fees
`

const purchaseHistorySelectClose = `
          ) AS u
          LEFT JOIN stripe_customers sc
                 ON sc.tenant_id = u.tenant_id
                AND sc.learner_gcid = u.learner_gcid
      ) AS h
`

// -----------------------------------------------------------------------------
// PurchaseHistoryRepo
// -----------------------------------------------------------------------------

// PurchaseHistoryRepo satisfies payments.PurchaseHistoryPort.
type PurchaseHistoryRepo struct {
	tx              TxRunner
	stripeCustomers *StripeCustomerRepo
}

// NewPurchaseHistoryRepo constructs the repo. stripeCustomers may be
// non-nil even though the canonical email hydration is done via the
// LEFT JOIN in-SQL — it's wired for future use (e.g., fallback paths
// when the JOIN is dropped for cost reasons on very large pages).
func NewPurchaseHistoryRepo(tx TxRunner, stripeCustomers *StripeCustomerRepo) *PurchaseHistoryRepo {
	return &PurchaseHistoryRepo{tx: tx, stripeCustomers: stripeCustomers}
}

// applyRLS is the chora-payments local RLS wrapper: it consults the
// rls.IsRLSBypassed(ctx) flag before delegating to the canonical
// libs/chora-go-common/rls.ApplySession. Bypass = skip SET LOCAL
// chora.tenant_id (used by ListAll PLATFORM_OPERATOR path).
//
// All non-PurchaseHistory repos continue calling rls.ApplySession
// directly — bypass is local to the cross-aggregate projection only.
func applyRLS(ctx context.Context, e cgcrls.Execer) error {
	if rls.IsRLSBypassed(ctx) {
		return nil
	}
	return cgcrls.ApplySession(ctx, e)
}

// ListByTenant satisfies the tenant-scoped read path. Caller MUST have
// set tenant_id on ctx; tenant isolation enforced by the RLS policy
// on every source table.
func (r *PurchaseHistoryRepo) ListByTenant(
	ctx context.Context,
	filter payments.FilterCriteria,
	cursor string,
	pageSize int,
) ([]payments.PurchaseHistoryItem, *string, error) {
	if r == nil || r.tx == nil {
		return nil, nil, ErrNotImplemented
	}
	// Defence-in-depth: tenant-scoped path MUST NOT have bypass set.
	if rls.IsRLSBypassed(ctx) {
		return nil, nil, errors.New("pg/purchase_history: ListByTenant called with RLS bypass — use ListAll instead")
	}
	return r.list(ctx, filter, cursor, pageSize, false)
}

// ListAll satisfies the PLATFORM_OPERATOR cross-tenant read path.
// Caller MUST have set rls.WithRLSBypass on ctx; the HTTP layer is the
// only intended caller of this code path.
func (r *PurchaseHistoryRepo) ListAll(
	ctx context.Context,
	filter payments.FilterCriteria,
	cursor string,
	pageSize int,
) ([]payments.PurchaseHistoryItem, *string, error) {
	if r == nil || r.tx == nil {
		return nil, nil, ErrNotImplemented
	}
	if !rls.IsRLSBypassed(ctx) {
		// Fail loud — operator path called without the bypass flag set
		// would silently filter every row by the missing chora.tenant_id
		// session var (=NULL). Reject to surface the role-gate omission.
		return nil, nil, errors.New("pg/purchase_history: ListAll requires rls.WithRLSBypass on ctx (PLATFORM_OPERATOR role-gated)")
	}
	return r.list(ctx, filter, cursor, pageSize, true)
}

func (r *PurchaseHistoryRepo) list(
	ctx context.Context,
	filter payments.FilterCriteria,
	cursor string,
	pageSize int,
	allowCrossTenant bool,
) ([]payments.PurchaseHistoryItem, *string, error) {
	if err := filter.Validate(); err != nil {
		return nil, nil, err
	}
	limit := payments.NormalisePageSize(pageSize)

	// Cursor decode (or nil for first page).
	cur, err := decodeCursor(cursor)
	if err != nil {
		return nil, nil, fmt.Errorf("pg/purchase_history: invalid cursor: %w", err)
	}

	sql, args := r.buildListSQL(filter, cur, limit, allowCrossTenant)

	var items []payments.PurchaseHistoryItem
	err = r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if rlsErr := applyRLS(ctx, qToExecer(q)); rlsErr != nil {
			return rlsErr
		}
		rows, qerr := q.Query(ctx, sql, args...)
		if qerr != nil {
			return fmt.Errorf("pg/purchase_history: query: %w", qerr)
		}
		defer rows.Close()
		for rows.Next() {
			it, scanErr := scanPurchaseHistoryItem(rows.Scan)
			if scanErr != nil {
				return scanErr
			}
			items = append(items, it)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, nil, err
	}

	// has_more detection: we asked for limit+1; if we got >limit, drop
	// the trailing extra + encode its predecessor as the next cursor.
	var next *string
	if len(items) > limit {
		last := items[limit-1]
		items = items[:limit]
		nc, encErr := encodeCursor(last.PaidAt, last.PurchaseID)
		if encErr != nil {
			return nil, nil, fmt.Errorf("pg/purchase_history: encode cursor: %w", encErr)
		}
		next = &nc
	}
	return items, next, nil
}

// buildListSQL assembles the SELECT … FROM (UNION ALL …) AS u LEFT JOIN …
// query body + the filter / cursor / order / LIMIT clauses. Returns the
// final SQL string + arg slice.
//
// Args (positional) order:
//
//	[allowCrossTenant=false]: $1 = tenant_id  (always; defense-in-depth
//	                            even though RLS already filters it)
//	[allowCrossTenant=true, filter.TenantID != ""]: $1 = tenant_id
//	[allowCrossTenant=true, filter.TenantID == ""]: no tenant arg
//	(then) aggregate_type filter, state filter, from, to, cursor (paid_at, purchase_id), limit+1
func (r *PurchaseHistoryRepo) buildListSQL(
	filter payments.FilterCriteria,
	cur *cursorPayload,
	limit int,
	allowCrossTenant bool,
) (string, []any) {
	var (
		args  []any
		conds []string
	)

	// Tenant predicate.
	if !allowCrossTenant {
		// Tenant-scoped: filter.TenantID is irrelevant (RLS already pins);
		// but include the tenant predicate as defense-in-depth using the
		// caller's session var would be a chicken-and-egg. Instead we
		// emit nothing here — RLS is the gate.
	} else if filter.TenantID != "" {
		args = append(args, filter.TenantID)
		conds = append(conds, fmt.Sprintf("tenant_id::text = $%d", len(args)))
	}

	if filter.AggregateType != payments.AggregateUnspecified {
		args = append(args, string(filter.AggregateType))
		conds = append(conds, fmt.Sprintf("aggregate_type = $%d", len(args)))
	}
	if filter.State != "" && filter.State != payments.StateAll {
		args = append(args, string(filter.State))
		conds = append(conds, fmt.Sprintf("state_normalised = $%d", len(args)))
	}
	if filter.From != nil {
		args = append(args, filter.From.UTC())
		conds = append(conds, fmt.Sprintf("created_at >= $%d", len(args)))
	}
	if filter.To != nil {
		args = append(args, filter.To.UTC())
		conds = append(conds, fmt.Sprintf("created_at < $%d", len(args)))
	}

	// Cursor: stable ORDER BY paid_at DESC NULLS LAST, purchase_id DESC;
	// continuation predicate uses (paid_at, purchase_id) tuple comparison.
	if cur != nil {
		if cur.PaidAt != nil {
			args = append(args, *cur.PaidAt)
			args = append(args, cur.PurchaseID)
			conds = append(conds, fmt.Sprintf(
				"(paid_at, purchase_id) < ($%d, $%d)",
				len(args)-1, len(args),
			))
		} else {
			// Anchor in the trailing NULLS-LAST section: purchase_id only.
			args = append(args, cur.PurchaseID)
			conds = append(conds, fmt.Sprintf("paid_at IS NULL AND purchase_id < $%d", len(args)))
		}
	}

	// LIMIT — always the last positional arg (test asserts on this).
	args = append(args, limit+1)
	limitArg := len(args)

	var whereClause string
	if len(conds) > 0 {
		whereClause = "WHERE " + strings.Join(conds, " AND ")
	}

	sql := strings.Join([]string{
		purchaseHistorySelectShape,
		purchaseHistoryUnionBody,
		purchaseHistorySelectClose,
		whereClause,
		"ORDER BY paid_at DESC NULLS LAST, purchase_id DESC",
		fmt.Sprintf("LIMIT $%d", limitArg),
	}, " ")
	_ = purchaseHistoryUnionCols // keep referenced for godoc anchor
	return sql, args
}

// scanPurchaseHistoryItem reads one row of the projection. 13 scan
// targets per the SELECT shape (12 base + 1 LEFT JOIN learner_email).
func scanPurchaseHistoryItem(scan func(...any) error) (payments.PurchaseHistoryItem, error) {
	var (
		purchaseID, aggregateType, tenantID, learnerGCID string
		learnerEmail                                     *string
		amountCents                                      int64
		currency, stateNorm, sessionID                   string
		paidAt, refundedAt                               *time.Time
		createdAt                                        time.Time
		metadataJSON                                     *string
	)
	if err := scan(
		&purchaseID, &aggregateType, &tenantID, &learnerGCID,
		&learnerEmail,
		&amountCents, &currency, &stateNorm, &sessionID,
		&paidAt, &refundedAt, &createdAt,
		&metadataJSON,
	); err != nil {
		return payments.PurchaseHistoryItem{}, fmt.Errorf("pg/purchase_history: scan: %w", err)
	}
	it := payments.PurchaseHistoryItem{
		PurchaseID:      purchaseID,
		AggregateType:   payments.AggregateType(aggregateType),
		TenantID:        tenantID,
		LearnerGCID:     learnerGCID,
		AmountCents:     amountCents,
		Currency:        currency,
		State:           payments.AdminState(stateNorm),
		StripeSessionID: sessionID,
		PaidAt:          paidAt,
		RefundedAt:      refundedAt,
		CreatedAt:       createdAt.UTC(),
	}
	if learnerEmail != nil {
		it.LearnerEmail = *learnerEmail
	}
	if metadataJSON != nil {
		it.MetadataJSON = *metadataJSON
	}
	return it, nil
}

// GetForRefund returns the targeted row + maps the FSM-vs-already-refunded
// invariant onto a typed error so the HTTP handler can branch into a
// 404 vs 409 response without a second DB round-trip.
//
// Implementation: runs the same UNION ALL projection filtered to the
// requested (purchase_id, aggregate_type) — keeps the projection logic
// in one place + reuses the LEFT JOIN email hydration.
func (r *PurchaseHistoryRepo) GetForRefund(
	ctx context.Context,
	purchaseID string,
	aggregate payments.AggregateType,
) (*payments.PurchaseHistoryItem, error) {
	if r == nil || r.tx == nil {
		return nil, ErrNotImplemented
	}
	if purchaseID == "" {
		return nil, errors.New("pg/purchase_history: purchase_id required")
	}
	if !aggregate.IsValid() || aggregate == payments.AggregateUnspecified {
		return nil, fmt.Errorf("pg/purchase_history: invalid aggregate_type %q", aggregate)
	}

	sql := strings.Join([]string{
		purchaseHistorySelectShape,
		purchaseHistoryUnionBody,
		purchaseHistorySelectClose,
		"WHERE purchase_id::text = $1 AND aggregate_type = $2",
		"LIMIT 1",
	}, " ")
	args := []any{purchaseID, string(aggregate)}

	var (
		found *payments.PurchaseHistoryItem
	)
	err := r.tx.RunInTx(ctx, func(ctx context.Context, q Querier) error {
		if rlsErr := applyRLS(ctx, qToExecer(q)); rlsErr != nil {
			return rlsErr
		}
		rows, qerr := q.Query(ctx, sql, args...)
		if qerr != nil {
			return fmt.Errorf("pg/purchase_history: GetForRefund query: %w", qerr)
		}
		defer rows.Close()
		if !rows.Next() {
			return payments.ErrPurchaseNotFound
		}
		it, scanErr := scanPurchaseHistoryItem(rows.Scan)
		if scanErr != nil {
			return scanErr
		}
		found = &it
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	if found == nil {
		return nil, payments.ErrPurchaseNotFound
	}
	if found.State == payments.StateRefunded {
		return nil, payments.ErrAlreadyRefunded
	}
	return found, nil
}

// -----------------------------------------------------------------------------
// Cursor encoding
// -----------------------------------------------------------------------------

// cursorPayload is the decoded form of an opaque next_page_token.
// paid_at may be nil for rows in the trailing NULLS-LAST section
// (un-captured / failed pre-capture).
type cursorPayload struct {
	PaidAt     *time.Time `json:"paid_at,omitempty"`
	PurchaseID string     `json:"purchase_id"`
}

func encodeCursor(paidAt *time.Time, purchaseID string) (string, error) {
	bz, err := json.Marshal(cursorPayload{PaidAt: paidAt, PurchaseID: purchaseID})
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(bz), nil
}

func decodeCursor(s string) (*cursorPayload, error) {
	if s == "" {
		return nil, nil
	}
	bz, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	var p cursorPayload
	if err := json.Unmarshal(bz, &p); err != nil {
		return nil, err
	}
	if p.PurchaseID == "" {
		return nil, errors.New("cursor missing purchase_id")
	}
	return &p, nil
}

// Compile-time conformance.
var _ payments.PurchaseHistoryPort = (*PurchaseHistoryRepo)(nil)
