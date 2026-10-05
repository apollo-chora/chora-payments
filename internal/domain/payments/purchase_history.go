// Package payments is the cross-aggregate domain layer for the H+
// tenant-admin Transaction History surface (ADR-164 §4 + plan
// .claude/plans/logical-growing-cat.md Phase 2 A1).
//
// Owns the PurchaseHistoryPort hexagonal port: a single read-only
// abstraction over the 7 Purchase aggregates' UNION-ALL projection
// (course_purchase + application_payment + familiar_egg_purchase +
// tenant_mana_topup + user_subscription + user_mana_topup +
// identity_kyc_fee).
//
// HARD RULE per .claude/rules/ddd-enforcement.md: this projection reads
// ONLY chora_payments tables. Cross-DB queries forbidden. Email
// hydration from chora_identity is via the on-payments-side
// stripe_customers registry (NOT a cross-DB join).
package payments

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// AggregateType identifies which of the 7 Purchase tables a row originated
// from. Mirrors paymentsv1.AggregateType + the OpenAPI string-enum;
// kept as a domain-level Go string so adapters can switch on it without
// importing the proto package.
type AggregateType string

const (
	AggregateUnspecified         AggregateType = ""
	AggregateCoursePurchase      AggregateType = "course_purchase"
	AggregateApplicationPayment  AggregateType = "application_payment"
	AggregateFamiliarEggPurchase AggregateType = "familiar_egg_purchase"
	AggregateTenantManaTopUp     AggregateType = "tenant_mana_topup"
	AggregateUserSubscription    AggregateType = "user_subscription"
	AggregateUserManaTopUp       AggregateType = "user_mana_topup"
	AggregateIdentityKycFee      AggregateType = "identity_kyc_fee"
)

// IsValid reports whether a is one of the 7 V1 admin aggregates (plus
// the unspecified zero-value).
func (a AggregateType) IsValid() bool {
	switch a {
	case AggregateUnspecified, AggregateCoursePurchase, AggregateApplicationPayment,
		AggregateFamiliarEggPurchase, AggregateTenantManaTopUp, AggregateUserSubscription,
		AggregateUserManaTopUp, AggregateIdentityKycFee:
		return true
	}
	return false
}

// AdminState is the normalised 4-value state mirroring openapi/payments-admin.yaml's
// PurchaseState enum. The underlying shared.State FSM has 5 values; we
// collapse `checkout_started` to `failed` at projection time so the H+
// surface stays user-facing.
type AdminState string

const (
	StateCaptured AdminState = "captured"
	StateRefunded AdminState = "refunded"
	StateFailed   AdminState = "failed"
	StateExpired  AdminState = "expired"
	StateAll      AdminState = "all"
)

// IsValid reports whether s is one of the normalised admin states (or
// the empty zero-value, which the adapter treats as "no filter").
func (s AdminState) IsValid() bool {
	switch s {
	case StateCaptured, StateRefunded, StateFailed, StateExpired, StateAll, AdminState(""):
		return true
	}
	return false
}

// AdminStateFromShared collapses the underlying 5-state shared.State FSM
// (carried verbatim in the SQL `state` column) into the H+ 4-state user
// surface. Mirrors the SQL CASE in adapter/repo/pg/purchase_history.go;
// kept in Go too so non-DB callers (in-mem stubs, projection tests) stay
// consistent with the canonical mapping.
//
// `checkout_started` collapses to `failed` per the OpenAPI note: the FE
// renders in-flight rows as failed once they age out, otherwise they
// flip to `captured` once the webhook lands.
func AdminStateFromShared(s string) AdminState {
	switch s {
	case "payment_captured":
		return StateCaptured
	case "refunded":
		return StateRefunded
	case "payment_failed":
		return StateFailed
	case "expired":
		return StateExpired
	case "checkout_started":
		return StateFailed
	}
	return AdminState("")
}

// PurchaseHistoryItem is a single row of the cross-aggregate UNION ALL
// projection. Fields chosen to be a strict superset of the columns the
// H+ FE renders and the export endpoint streams.
type PurchaseHistoryItem struct {
	PurchaseID    string
	AggregateType AggregateType
	TenantID      string
	LearnerGCID   string
	// LearnerEmail is hydrated via stripe_customers JOIN in the same
	// SQL statement. Empty when the GCID has been pseudonymised
	// post-closure crypto-shred (per ADR-149 + closure saga).
	LearnerEmail    string
	AmountCents     int64
	Currency        string
	State           AdminState
	StripeSessionID string
	PaidAt          *time.Time
	RefundedAt      *time.Time
	CreatedAt       time.Time
	// MetadataJSON is a free-form aggregate-specific projection encoded
	// as compact JSON. Examples:
	//   - course_purchase:    {"course_id":"…"}
	//   - tenant_mana_topup:  {"sku":"…","mana_units":…}
	//   - user_subscription:  {"plan_sku":"…","billing_period":"monthly"}
	// The FE renders the relevant subset per aggregate_type.
	MetadataJSON string
}

// FilterCriteria carries the query parameters for ListByTenant + ListAll +
// ExportPurchases. Zero-value = unfiltered.
type FilterCriteria struct {
	// AggregateType narrows to a single aggregate; AggregateUnspecified
	// spans all 7.
	AggregateType AggregateType
	// State narrows to a single normalised state; StateAll or empty
	// returns rows in any state.
	State AdminState
	// From is the inclusive lower bound on Purchase.created_at.
	From *time.Time
	// To is the exclusive upper bound on Purchase.created_at.
	To *time.Time
	// TenantID — PLATFORM_OPERATOR-only narrowing. Tenant-scoped callers
	// have this overridden at the HTTP layer to the caller's own tenant
	// (see internal/adapter/http/admin_handler.go).
	TenantID string
}

// Validate enforces the cheap domain invariants — caller-supplied filter
// values must be well-formed. Real RLS / cross-tenant scope is enforced
// at the HTTP / RLS-bypass layer; this method only screens out shapes
// that would never produce sensible SQL.
func (f FilterCriteria) Validate() error {
	if !f.AggregateType.IsValid() {
		return fmt.Errorf("payments: invalid aggregate_type %q", f.AggregateType)
	}
	if !f.State.IsValid() {
		return fmt.Errorf("payments: invalid state %q", f.State)
	}
	if f.From != nil && f.To != nil && f.From.After(*f.To) {
		return errors.New("payments: from must be <= to (window cannot be backward)")
	}
	return nil
}

// PageSizeDefault is the page_size value used when caller omits it.
// Mirrors the OpenAPI default + PaymentsAdminService gRPC parity rule.
const PageSizeDefault = 20

// PageSizeMax is the largest page_size accepted. Mirrors the OpenAPI
// enum (10, 20, 50, 100) cap.
const PageSizeMax = 100

// NormalisePageSize clamps the caller-supplied page_size into the
// allowed window. Anything <=0 falls back to PageSizeDefault.
func NormalisePageSize(n int) int {
	if n <= 0 {
		return PageSizeDefault
	}
	if n > PageSizeMax {
		return PageSizeMax
	}
	return n
}

// Errors surfaced by the port (shared by adapters).
var (
	// ErrPurchaseNotFound — the lookup (e.g. for refund pre-flight) found
	// no row matching (purchase_id, aggregate_type).
	ErrPurchaseNotFound = errors.New("payments/history: purchase not found")

	// ErrAlreadyRefunded — the targeted Purchase is already in `refunded`
	// state; the refund handler returns 409 to the HTTP caller per the
	// OpenAPI contract.
	ErrAlreadyRefunded = errors.New("payments/history: purchase already refunded")
)

// PurchaseHistoryPort is the hexagonal port the H+ admin handlers depend
// on. Implementations: pg-backed (adapter/repo/pg/purchase_history.go)
// is the canonical production wire; in-mem stub is left to a follow-up.
type PurchaseHistoryPort interface {
	// ListByTenant is the tenant-scoped read path. Caller MUST have set
	// tenant_id on ctx via tracing.WithTenantID; the underlying RLS
	// policy enforces the per-tenant filter (NO bypass).
	//
	// Cursor: empty string on the first call. nextCursor returned is nil
	// when there are no more rows (has_more=false at the HTTP layer).
	ListByTenant(
		ctx context.Context,
		filter FilterCriteria,
		cursor string,
		pageSize int,
	) (items []PurchaseHistoryItem, nextCursor *string, err error)

	// ListAll is the PLATFORM_OPERATOR cross-tenant read path. Caller MUST
	// have set the RLS-bypass flag on ctx via
	// internal/adapter/repo/rls.WithRLSBypass + asserted the caller's
	// role is PLATFORM_OPERATOR.
	//
	// If filter.TenantID is set, the result is narrowed to that tenant;
	// empty filter.TenantID spans every tenant.
	ListAll(
		ctx context.Context,
		filter FilterCriteria,
		cursor string,
		pageSize int,
	) (items []PurchaseHistoryItem, nextCursor *string, err error)

	// GetForRefund is the pre-refund lookup. Returns ErrPurchaseNotFound
	// when no row matches; ErrAlreadyRefunded when the row is already
	// in `refunded` state (so the HTTP layer can return 409 without an
	// extra DB round-trip).
	GetForRefund(
		ctx context.Context,
		purchaseID string,
		aggregate AggregateType,
	) (*PurchaseHistoryItem, error)
}
