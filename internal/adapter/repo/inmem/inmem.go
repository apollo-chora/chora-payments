// Package inmem provides in-memory implementations of the 5 Purchase
// aggregate Repos + the webhook_event Repo. Used:
//
//   - In unit tests (test fixture wiring).
//   - In local dev when CHORA_DB_DSN_SECRET_ID is unset — chora-payments
//     boot logs "WARNING: CHORA_DB_DSN_SECRET_ID unset — using in-memory
//     repos" so the gap is visible.
//
// All repos are goroutine-safe via a sync.RWMutex per repo.
package inmem

import (
	"context"
	"strings"
	"sync"
	"time"

	apppay "github.com/apollo-chora/chora-payments/internal/domain/application_payment"
	"github.com/apollo-chora/chora-payments/internal/domain/coursepurchase"
	"github.com/apollo-chora/chora-payments/internal/domain/dispute"
	egg "github.com/apollo-chora/chora-payments/internal/domain/familiar_egg_purchase"
	kyc "github.com/apollo-chora/chora-payments/internal/domain/identity_kyc_fee"
	"github.com/apollo-chora/chora-payments/internal/domain/shared"
	stripecustomer "github.com/apollo-chora/chora-payments/internal/domain/stripe_customer"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
	mana "github.com/apollo-chora/chora-payments/internal/domain/tenant_mana_topup"
	umt "github.com/apollo-chora/chora-payments/internal/domain/user_mana_topup"
	sub "github.com/apollo-chora/chora-payments/internal/domain/user_subscription"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

// -----------------------------------------------------------------------------
// CoursePurchaseRepo
// -----------------------------------------------------------------------------

type CoursePurchaseRepo struct {
	mu     sync.RWMutex
	byID   map[string]*coursepurchase.CoursePurchase // purchase_id
	bySess map[string]string                         // stripe_session_id -> purchase_id
}

func NewCoursePurchaseRepo() *CoursePurchaseRepo {
	return &CoursePurchaseRepo{
		byID:   make(map[string]*coursepurchase.CoursePurchase),
		bySess: make(map[string]string),
	}
}

func (r *CoursePurchaseRepo) Save(_ context.Context, cp *coursepurchase.CoursePurchase) error {
	if cp == nil {
		return shared.ErrNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[cp.PurchaseID] = cp
	if cp.StripeSessionID != "" {
		r.bySess[cp.StripeSessionID] = cp.PurchaseID
	}
	return nil
}

func (r *CoursePurchaseRepo) GetByID(_ context.Context, tenantID, purchaseID string) (*coursepurchase.CoursePurchase, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cp, ok := r.byID[purchaseID]
	if !ok || cp.TenantID != tenantID {
		return nil, shared.ErrNotFound
	}
	return cp, nil
}

func (r *CoursePurchaseRepo) GetByStripeSessionID(_ context.Context, sessID string) (*coursepurchase.CoursePurchase, error) {
	if strings.TrimSpace(sessID) == "" {
		return nil, shared.ErrStripeSessionRequired
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	pid, ok := r.bySess[sessID]
	if !ok {
		return nil, shared.ErrNotFound
	}
	cp, ok := r.byID[pid]
	if !ok {
		return nil, shared.ErrNotFound
	}
	return cp, nil
}

var _ coursepurchase.Repo = (*CoursePurchaseRepo)(nil)

// -----------------------------------------------------------------------------
// ApplicationPaymentRepo
// -----------------------------------------------------------------------------

type ApplicationPaymentRepo struct {
	mu     sync.RWMutex
	byID   map[string]*apppay.ApplicationPayment
	bySess map[string]string
}

func NewApplicationPaymentRepo() *ApplicationPaymentRepo {
	return &ApplicationPaymentRepo{
		byID:   make(map[string]*apppay.ApplicationPayment),
		bySess: make(map[string]string),
	}
}

func (r *ApplicationPaymentRepo) Save(_ context.Context, ap *apppay.ApplicationPayment) error {
	if ap == nil {
		return shared.ErrNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[ap.PurchaseID] = ap
	if ap.StripeSessionID != "" {
		r.bySess[ap.StripeSessionID] = ap.PurchaseID
	}
	return nil
}

func (r *ApplicationPaymentRepo) GetByID(_ context.Context, tenantID, purchaseID string) (*apppay.ApplicationPayment, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ap, ok := r.byID[purchaseID]
	if !ok || ap.TenantID != tenantID {
		return nil, shared.ErrNotFound
	}
	return ap, nil
}

func (r *ApplicationPaymentRepo) GetByStripeSessionID(_ context.Context, sessID string) (*apppay.ApplicationPayment, error) {
	if strings.TrimSpace(sessID) == "" {
		return nil, shared.ErrStripeSessionRequired
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	pid, ok := r.bySess[sessID]
	if !ok {
		return nil, shared.ErrNotFound
	}
	return r.byID[pid], nil
}

var _ apppay.Repo = (*ApplicationPaymentRepo)(nil)

// -----------------------------------------------------------------------------
// FamiliarEggPurchaseRepo
// -----------------------------------------------------------------------------

type FamiliarEggPurchaseRepo struct {
	mu     sync.RWMutex
	byID   map[string]*egg.FamiliarEggPurchase
	bySess map[string]string
}

func NewFamiliarEggPurchaseRepo() *FamiliarEggPurchaseRepo {
	return &FamiliarEggPurchaseRepo{
		byID:   make(map[string]*egg.FamiliarEggPurchase),
		bySess: make(map[string]string),
	}
}

func (r *FamiliarEggPurchaseRepo) Save(_ context.Context, ep *egg.FamiliarEggPurchase) error {
	if ep == nil {
		return shared.ErrNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[ep.PurchaseID] = ep
	if ep.StripeSessionID != "" {
		r.bySess[ep.StripeSessionID] = ep.PurchaseID
	}
	return nil
}

func (r *FamiliarEggPurchaseRepo) GetByID(_ context.Context, tenantID, purchaseID string) (*egg.FamiliarEggPurchase, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ep, ok := r.byID[purchaseID]
	if !ok || ep.TenantID != tenantID {
		return nil, shared.ErrNotFound
	}
	return ep, nil
}

func (r *FamiliarEggPurchaseRepo) GetByStripeSessionID(_ context.Context, sessID string) (*egg.FamiliarEggPurchase, error) {
	if strings.TrimSpace(sessID) == "" {
		return nil, shared.ErrStripeSessionRequired
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	pid, ok := r.bySess[sessID]
	if !ok {
		return nil, shared.ErrNotFound
	}
	return r.byID[pid], nil
}

var _ egg.Repo = (*FamiliarEggPurchaseRepo)(nil)

// -----------------------------------------------------------------------------
// TenantManaTopUpRepo
// -----------------------------------------------------------------------------

type TenantManaTopUpRepo struct {
	mu     sync.RWMutex
	byID   map[string]*mana.TenantManaTopUp
	bySess map[string]string
}

func NewTenantManaTopUpRepo() *TenantManaTopUpRepo {
	return &TenantManaTopUpRepo{
		byID:   make(map[string]*mana.TenantManaTopUp),
		bySess: make(map[string]string),
	}
}

func (r *TenantManaTopUpRepo) Save(_ context.Context, t *mana.TenantManaTopUp) error {
	if t == nil {
		return shared.ErrNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[t.PurchaseID] = t
	if t.StripeSessionID != "" {
		r.bySess[t.StripeSessionID] = t.PurchaseID
	}
	return nil
}

func (r *TenantManaTopUpRepo) GetByID(_ context.Context, tenantID, purchaseID string) (*mana.TenantManaTopUp, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.byID[purchaseID]
	if !ok || t.TenantID != tenantID {
		return nil, shared.ErrNotFound
	}
	return t, nil
}

func (r *TenantManaTopUpRepo) GetByStripeSessionID(_ context.Context, sessID string) (*mana.TenantManaTopUp, error) {
	if strings.TrimSpace(sessID) == "" {
		return nil, shared.ErrStripeSessionRequired
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	pid, ok := r.bySess[sessID]
	if !ok {
		return nil, shared.ErrNotFound
	}
	return r.byID[pid], nil
}

var _ mana.Repo = (*TenantManaTopUpRepo)(nil)

// -----------------------------------------------------------------------------
// UserSubscriptionRepo
// -----------------------------------------------------------------------------

type UserSubscriptionRepo struct {
	mu      sync.RWMutex
	byID    map[string]*sub.UserSubscription
	bySess  map[string]string
	bySubID map[string]string // stripe_subscription_id → purchase_id
}

func NewUserSubscriptionRepo() *UserSubscriptionRepo {
	return &UserSubscriptionRepo{
		byID:    make(map[string]*sub.UserSubscription),
		bySess:  make(map[string]string),
		bySubID: make(map[string]string),
	}
}

func (r *UserSubscriptionRepo) Save(_ context.Context, u *sub.UserSubscription) error {
	if u == nil {
		return shared.ErrNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[u.PurchaseID] = u
	if u.StripeSessionID != "" {
		r.bySess[u.StripeSessionID] = u.PurchaseID
	}
	if u.StripeSubscriptionID != "" {
		r.bySubID[u.StripeSubscriptionID] = u.PurchaseID
	}
	return nil
}

func (r *UserSubscriptionRepo) GetByID(_ context.Context, tenantID, purchaseID string) (*sub.UserSubscription, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	u, ok := r.byID[purchaseID]
	if !ok || u.TenantID != tenantID {
		return nil, shared.ErrNotFound
	}
	return u, nil
}

func (r *UserSubscriptionRepo) GetByStripeSessionID(_ context.Context, sessID string) (*sub.UserSubscription, error) {
	if strings.TrimSpace(sessID) == "" {
		return nil, shared.ErrStripeSessionRequired
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	pid, ok := r.bySess[sessID]
	if !ok {
		return nil, shared.ErrNotFound
	}
	return r.byID[pid], nil
}

func (r *UserSubscriptionRepo) GetByStripeSubscriptionID(_ context.Context, subID string) (*sub.UserSubscription, error) {
	if strings.TrimSpace(subID) == "" {
		return nil, shared.ErrNotFound
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	pid, ok := r.bySubID[subID]
	if !ok {
		return nil, shared.ErrNotFound
	}
	return r.byID[pid], nil
}

var _ sub.Repo = (*UserSubscriptionRepo)(nil)

// -----------------------------------------------------------------------------
// WebhookEventRepo — global dedup
// -----------------------------------------------------------------------------

type WebhookEventRepo struct {
	mu sync.Mutex
	m  map[string]*wh.WebhookEvent // event_id → row
}

func NewWebhookEventRepo() *WebhookEventRepo {
	return &WebhookEventRepo{m: make(map[string]*wh.WebhookEvent)}
}

func (r *WebhookEventRepo) Insert(_ context.Context, w *wh.WebhookEvent) error {
	if w == nil {
		return wh.ErrEventIDRequired
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.m[w.EventID]; ok {
		return wh.ErrAlreadyProcessed
	}
	r.m[w.EventID] = w
	return nil
}

func (r *WebhookEventRepo) MarkProcessed(_ context.Context, eventID string, agg wh.AggregateType, purchaseID string, processedAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.m[eventID]
	if !ok {
		return shared.ErrNotFound
	}
	w.TargetAggregateType = agg
	w.TargetPurchaseID = purchaseID
	w.ProcessedAt = &processedAt
	return nil
}

func (r *WebhookEventRepo) MarkFailed(_ context.Context, eventID, processingError string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.m[eventID]
	if !ok {
		return shared.ErrNotFound
	}
	w.ProcessingError = processingError
	return nil
}

func (r *WebhookEventRepo) GetByEventID(_ context.Context, eventID string) (*wh.WebhookEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.m[eventID]
	if !ok {
		return nil, shared.ErrNotFound
	}
	return w, nil
}

var _ wh.Repo = (*WebhookEventRepo)(nil)

// -----------------------------------------------------------------------------
// StripeCustomerRepo — (tenant_id, learner_gcid) → cus_xxx registry
// -----------------------------------------------------------------------------

type StripeCustomerRepo struct {
	mu sync.RWMutex
	// key = tenantID + "|" + learnerGCID
	m map[string]*stripecustomer.StripeCustomer
}

func NewStripeCustomerRepo() *StripeCustomerRepo {
	return &StripeCustomerRepo{m: make(map[string]*stripecustomer.StripeCustomer)}
}

func stripeCustomerKey(tenantID, learnerGCID string) string {
	return tenantID + "|" + learnerGCID
}

func (r *StripeCustomerRepo) GetByGCID(_ context.Context, tenantID, learnerGCID string) (*stripecustomer.StripeCustomer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sc, ok := r.m[stripeCustomerKey(tenantID, learnerGCID)]
	if !ok {
		return nil, stripecustomer.ErrNotFound
	}
	return sc, nil
}

func (r *StripeCustomerRepo) Insert(_ context.Context, sc *stripecustomer.StripeCustomer) error {
	if sc == nil {
		return stripecustomer.ErrStripeCustomerEmpty
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := stripeCustomerKey(sc.TenantID, sc.LearnerGCID)
	if _, exists := r.m[key]; exists {
		// Idempotent: existing row wins.
		return nil
	}
	r.m[key] = sc
	return nil
}

// GetByGCIDs satisfies the batch-hydration port the H+ Transaction History
// projection consumes. Missing GCIDs are silently omitted.
func (r *StripeCustomerRepo) GetByGCIDs(_ context.Context, tenantID string, gcids []string) (map[string]*stripecustomer.StripeCustomer, error) {
	out := make(map[string]*stripecustomer.StripeCustomer, len(gcids))
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, gcid := range gcids {
		if gcid == "" {
			continue
		}
		sc, ok := r.m[stripeCustomerKey(tenantID, gcid)]
		if !ok {
			continue
		}
		out[gcid] = sc
	}
	return out, nil
}

var _ stripecustomer.Repo = (*StripeCustomerRepo)(nil)

// -----------------------------------------------------------------------------
// UserManaTopUpRepo (6th aggregate — Stage A.5)
// -----------------------------------------------------------------------------

type UserManaTopUpRepo struct {
	mu     sync.RWMutex
	byID   map[string]*umt.UserManaTopUp
	bySess map[string]string
}

func NewUserManaTopUpRepo() *UserManaTopUpRepo {
	return &UserManaTopUpRepo{
		byID:   make(map[string]*umt.UserManaTopUp),
		bySess: make(map[string]string),
	}
}

func (r *UserManaTopUpRepo) Save(_ context.Context, u *umt.UserManaTopUp) error {
	if u == nil {
		return shared.ErrNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[u.PurchaseID] = u
	if u.StripeSessionID != "" {
		r.bySess[u.StripeSessionID] = u.PurchaseID
	}
	return nil
}

func (r *UserManaTopUpRepo) GetByID(_ context.Context, tenantID, purchaseID string) (*umt.UserManaTopUp, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	u, ok := r.byID[purchaseID]
	if !ok || u.TenantID != tenantID {
		return nil, shared.ErrNotFound
	}
	return u, nil
}

func (r *UserManaTopUpRepo) GetByStripeSessionID(_ context.Context, sessID string) (*umt.UserManaTopUp, error) {
	if strings.TrimSpace(sessID) == "" {
		return nil, shared.ErrStripeSessionRequired
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	pid, ok := r.bySess[sessID]
	if !ok {
		return nil, shared.ErrNotFound
	}
	return r.byID[pid], nil
}

var _ umt.Repo = (*UserManaTopUpRepo)(nil)

// -----------------------------------------------------------------------------
// IdentityKycFeeRepo (7th aggregate — Stage A.5)
// -----------------------------------------------------------------------------

type IdentityKycFeeRepo struct {
	mu     sync.RWMutex
	byID   map[string]*kyc.IdentityKycFee
	bySess map[string]string
}

func NewIdentityKycFeeRepo() *IdentityKycFeeRepo {
	return &IdentityKycFeeRepo{
		byID:   make(map[string]*kyc.IdentityKycFee),
		bySess: make(map[string]string),
	}
}

func (r *IdentityKycFeeRepo) Save(_ context.Context, k *kyc.IdentityKycFee) error {
	if k == nil {
		return shared.ErrNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[k.PurchaseID] = k
	if k.StripeSessionID != "" {
		r.bySess[k.StripeSessionID] = k.PurchaseID
	}
	return nil
}

func (r *IdentityKycFeeRepo) GetByID(_ context.Context, tenantID, purchaseID string) (*kyc.IdentityKycFee, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	k, ok := r.byID[purchaseID]
	if !ok || k.TenantID != tenantID {
		return nil, shared.ErrNotFound
	}
	return k, nil
}

func (r *IdentityKycFeeRepo) GetByStripeSessionID(_ context.Context, sessID string) (*kyc.IdentityKycFee, error) {
	if strings.TrimSpace(sessID) == "" {
		return nil, shared.ErrStripeSessionRequired
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	pid, ok := r.bySess[sessID]
	if !ok {
		return nil, shared.ErrNotFound
	}
	return r.byID[pid], nil
}

var _ kyc.Repo = (*IdentityKycFeeRepo)(nil)

// -----------------------------------------------------------------------------
// DisputeRepo (ADR-164 §229.5 — chargeback aggregate)
// -----------------------------------------------------------------------------

type DisputeRepo struct {
	mu         sync.RWMutex
	byID       map[string]*dispute.Dispute // dispute_id
	byStripeID map[string]string           // stripe_dispute_id -> dispute_id
	byPurchase map[string][]string         // "{aggregate_type}|{purchase_id}" -> []dispute_id
}

func NewDisputeRepo() *DisputeRepo {
	return &DisputeRepo{
		byID:       make(map[string]*dispute.Dispute),
		byStripeID: make(map[string]string),
		byPurchase: make(map[string][]string),
	}
}

func (r *DisputeRepo) purchaseKey(aggregateType wh.AggregateType, purchaseID string) string {
	return string(aggregateType) + "|" + purchaseID
}

func (r *DisputeRepo) Insert(_ context.Context, d *dispute.Dispute) error {
	if d == nil {
		return shared.ErrNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if d.StripeDisputeID != "" {
		if _, exists := r.byStripeID[d.StripeDisputeID]; exists {
			return dispute.ErrAlreadyExists
		}
	}
	r.byID[d.DisputeID] = d
	if d.StripeDisputeID != "" {
		r.byStripeID[d.StripeDisputeID] = d.DisputeID
	}
	key := r.purchaseKey(d.AggregateType, d.PurchaseID)
	r.byPurchase[key] = append(r.byPurchase[key], d.DisputeID)
	return nil
}

func (r *DisputeRepo) Save(_ context.Context, d *dispute.Dispute) error {
	if d == nil {
		return shared.ErrNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[d.DisputeID] = d
	// Ensure index entries exist (Save may follow an Insert-then-mutate path
	// in the dispatcher, or arrive as an upsert from a replayed close event).
	if d.StripeDisputeID != "" {
		if _, ok := r.byStripeID[d.StripeDisputeID]; !ok {
			r.byStripeID[d.StripeDisputeID] = d.DisputeID
		}
	}
	key := r.purchaseKey(d.AggregateType, d.PurchaseID)
	if !stringSliceContains(r.byPurchase[key], d.DisputeID) {
		r.byPurchase[key] = append(r.byPurchase[key], d.DisputeID)
	}
	return nil
}

func (r *DisputeRepo) GetByStripeDisputeID(_ context.Context, stripeDisputeID string) (*dispute.Dispute, error) {
	if strings.TrimSpace(stripeDisputeID) == "" {
		return nil, dispute.ErrStripeDisputeRequired
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.byStripeID[stripeDisputeID]
	if !ok {
		return nil, dispute.ErrNotFound
	}
	d, ok := r.byID[id]
	if !ok {
		return nil, dispute.ErrNotFound
	}
	return d, nil
}

func (r *DisputeRepo) GetByID(_ context.Context, tenantID, disputeID string) (*dispute.Dispute, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.byID[disputeID]
	if !ok || d.TenantID != tenantID {
		return nil, dispute.ErrNotFound
	}
	return d, nil
}

func (r *DisputeRepo) ListByPurchase(_ context.Context, tenantID string, aggregateType wh.AggregateType, purchaseID string) ([]*dispute.Dispute, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := r.purchaseKey(aggregateType, purchaseID)
	ids := r.byPurchase[key]
	out := make([]*dispute.Dispute, 0, len(ids))
	for _, id := range ids {
		d := r.byID[id]
		if d == nil || d.TenantID != tenantID {
			continue
		}
		out = append(out, d)
	}
	return out, nil
}

func stringSliceContains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

var _ dispute.Repo = (*DisputeRepo)(nil)

// -----------------------------------------------------------------------------
// TenantAddonPurchaseRepo — 8th aggregate, CHO-1738.
// -----------------------------------------------------------------------------

type TenantAddonPurchaseRepo struct {
	mu        sync.RWMutex
	byID      map[string]*tap.TenantAddonPurchase
	bySess    map[string]string
	bySubID   map[string]string // CHO-1761: stripe_subscription_id → purchase_id
	bySchedID map[string]string // CHO-1772: stripe_subscription_schedule_id → purchase_id
}

func NewTenantAddonPurchaseRepo() *TenantAddonPurchaseRepo {
	return &TenantAddonPurchaseRepo{
		byID:      make(map[string]*tap.TenantAddonPurchase),
		bySess:    make(map[string]string),
		bySubID:   make(map[string]string),
		bySchedID: make(map[string]string),
	}
}

func (r *TenantAddonPurchaseRepo) Save(_ context.Context, a *tap.TenantAddonPurchase) error {
	if a == nil {
		return shared.ErrNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[a.PurchaseID] = a
	if a.StripeSessionID != "" {
		r.bySess[a.StripeSessionID] = a.PurchaseID
	}
	if a.StripeSubscriptionID != "" {
		r.bySubID[a.StripeSubscriptionID] = a.PurchaseID
	}
	// CHO-1772 — track schedule_id for the subscription_schedule.released
	// webhook lookup. Mirror pg's nullable-column semantics: when the row
	// no longer carries a schedule_id (ReleaseSchedule / ClearSchedule
	// happened upstream of this Save), evict the stale mapping so a
	// duplicate webhook delivery doesn't re-hit the cleared row.
	for sid, pid := range r.bySchedID {
		if pid == a.PurchaseID && sid != a.StripeSubscriptionScheduleID {
			delete(r.bySchedID, sid)
		}
	}
	if a.StripeSubscriptionScheduleID != "" {
		r.bySchedID[a.StripeSubscriptionScheduleID] = a.PurchaseID
	}
	return nil
}

func (r *TenantAddonPurchaseRepo) GetByID(_ context.Context, tenantID, purchaseID string) (*tap.TenantAddonPurchase, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.byID[purchaseID]
	if !ok || a.TenantID != tenantID {
		return nil, shared.ErrNotFound
	}
	return a, nil
}

func (r *TenantAddonPurchaseRepo) GetByStripeSessionID(_ context.Context, sessID string) (*tap.TenantAddonPurchase, error) {
	if strings.TrimSpace(sessID) == "" {
		return nil, shared.ErrStripeSessionRequired
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	pid, ok := r.bySess[sessID]
	if !ok {
		return nil, shared.ErrNotFound
	}
	return r.byID[pid], nil
}

// GetByStripeSubscriptionID — CHO-1761 webhook dispatch lookup.
// Empty subscriptionID fail-loud per ErrStripeSubscriptionRequired —
// otherwise an aggregate with no Stripe sub yet (legacy mode=payment
// row) would match the empty-string lookup, leaking cross-tenant.
func (r *TenantAddonPurchaseRepo) GetByStripeSubscriptionID(_ context.Context, subID string) (*tap.TenantAddonPurchase, error) {
	if strings.TrimSpace(subID) == "" {
		return nil, tap.ErrStripeSubscriptionRequired
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	pid, ok := r.bySubID[subID]
	if !ok {
		return nil, shared.ErrNotFound
	}
	return r.byID[pid], nil
}

// GetActiveByTenantAndAddonCode — CHO-1764. Returns the most-recently
// updated payment_captured row for (tenant, addon_code). Inmem scans
// all rows; production pg uses the new SQL in CHO-1764.
func (r *TenantAddonPurchaseRepo) GetActiveByTenantAndAddonCode(_ context.Context, tenantID, addonCode string) (*tap.TenantAddonPurchase, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, tap.ErrTenantRequired
	}
	if strings.TrimSpace(addonCode) == "" {
		return nil, tap.ErrAddonCodeRequired
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var best *tap.TenantAddonPurchase
	for _, a := range r.byID {
		if a.TenantID != tenantID || a.AddonCode != addonCode {
			continue
		}
		if a.State != shared.StatePaymentCaptured {
			continue
		}
		if best == nil || a.UpdatedAt.After(best.UpdatedAt) {
			best = a
		}
	}
	if best == nil {
		return nil, shared.ErrNotFound
	}
	return best, nil
}

// GetByStripeScheduleID — CHO-1772 webhook dispatch lookup. The
// stripe_subscription_schedule_id is UNIQUE + Stripe-issued, so the RLS
// scope filter is skipped the same way GetByStripeSubscriptionID does.
// Empty scheduleID fail-loud per ErrStripeScheduleRequired so a stale
// post-release row (where the column was nulled by ReleaseSchedule)
// doesn't satisfy a future empty-string lookup.
func (r *TenantAddonPurchaseRepo) GetByStripeScheduleID(_ context.Context, scheduleID string) (*tap.TenantAddonPurchase, error) {
	if strings.TrimSpace(scheduleID) == "" {
		return nil, tap.ErrStripeScheduleRequired
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	pid, ok := r.bySchedID[scheduleID]
	if !ok {
		return nil, shared.ErrNotFound
	}
	return r.byID[pid], nil
}

// GetStripeCustomerIDByTenant — H+ Billing. Returns the tenant's Stripe
// Customer ID (one Customer per tenant by CHO-1762 contract). Scans all
// rows for the tenant and returns the first non-empty stripe_customer_id;
// rows that haven't reached payment_captured (no Customer wired yet) are
// skipped so they don't shadow a sibling row that IS fully wired.
// Returns empty string + nil error on no match — billing renders empty
// invoice list, not 404.
func (r *TenantAddonPurchaseRepo) GetStripeCustomerIDByTenant(_ context.Context, tenantID string) (string, error) {
	if strings.TrimSpace(tenantID) == "" {
		return "", tap.ErrTenantRequired
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, a := range r.byID {
		if a.TenantID != tenantID {
			continue
		}
		if a.StripeCustomerID == "" {
			continue
		}
		return a.StripeCustomerID, nil
	}
	return "", nil
}

var _ tap.Repo = (*TenantAddonPurchaseRepo)(nil)
