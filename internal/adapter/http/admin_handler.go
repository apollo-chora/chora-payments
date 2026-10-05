// admin_handler.go — admin HTTP surface for chora-payments.
//
// Routes:
//
//	GET  /api/v1/payments/{purchase_id}?aggregate_type=...&tenant_id=...
//	POST /api/v1/payments/{purchase_id}/refund
//
// Role-gated upstream by `training-admin` per the chora-gateway BFF
// (BFF inspects the ChoraSession JWT + propagates X-Chora-Role).
// This handler trusts X-Chora-Role for defence-in-depth inside the
// service mesh; the canonical auth gate is chora-gateway.
//
// Tenant scope is required via X-Chora-Tenant-Id (BFF-set) or the
// `tenant_id` query parameter / JSON body. The handler propagates
// tenant_id into the gRPC request so the server-side repos apply RLS.
//
// Per ADR-164 §4.3.
package http

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	pb "github.com/apollo-chora/chora-contracts/gen/go/chora/services/payments/v1"

	"github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-payments/internal/adapter/outbox"
	"github.com/apollo-chora/chora-payments/internal/domain/payments"
)

// PaymentService is the gRPC server contract the admin HTTP layer
// invokes in-process.
type PaymentService interface {
	GetPurchase(ctx context.Context, in *pb.GetPurchaseRequest) (*pb.GetPurchaseResponse, error)
	RefundPurchase(ctx context.Context, in *pb.RefundPurchaseRequest) (*pb.RefundPurchaseResponse, error)
}

// AdminHandlerDeps wires the admin HTTP routes.
type AdminHandlerDeps struct {
	Payments PaymentService

	// AllowedRoles is the set of X-Chora-Role values accepted at the
	// admin endpoints. Defaults to {"training-admin", "platform-admin"}.
	AllowedRoles []string

	// History backs the 4 H+ Transaction History endpoints (ListPurchases,
	// RefundPurchase, ExportPurchases, StreamPaymentEvents). nil =
	// endpoints respond 501 (the handler tolerates the missing port so
	// existing GetPurchase + RefundPurchase keep working unchanged).
	History payments.PurchaseHistoryPort

	// AuditOutbox emits the 3 governance.audit.* events for IMDA D1
	// accountability. nil = audit emit is a no-op (the endpoint still
	// succeeds — fail-open by design so an outbox outage does not break
	// admin reads; production wiring is fail-loud via separate dashboard).
	AuditOutbox outbox.AuditOutbox

	// PaymentEvents is the SSE source for /stream. When nil, /stream
	// returns 503 (real fan-out source wiring is a follow-up; the HTTP
	// shape is in place so the FE can implement against it).
	PaymentEvents PaymentEventSource

	// Now is injectable for tests; defaults to time.Now().UTC.
	Now func() time.Time
}

// PaymentEventSource is the small port for the /stream endpoint. A
// real implementation fans out chora.payments.*.v1 Pub/Sub deliveries
// into a per-session channel; the tests use a controllable stub.
//
// A1.1 update — Subscribe returns a cleanup closure the handler MUST
// call on HTTP-client disconnect so the broker drops the channel + any
// pending events. Without cleanup the broker would leak subscriber
// slots forever (one per FE reconnect).
type PaymentEventSource interface {
	// Subscribe opens a per-session feed scoped by tenant (empty
	// tenantID = cross-tenant for PLATFORM_OPERATOR) and aggregate
	// (empty = all 7). The returned channel receives one PaymentEvent
	// per state transition until ctx is canceled. Caller MUST invoke
	// the cleanup closure on disconnect — calling it more than once
	// is a no-op.
	Subscribe(ctx context.Context, tenantID, aggregateType string) (<-chan PaymentEvent, func(), error)
}

// PaymentEvent is the wire shape served to SSE clients. Mirrors the
// payments-admin.yaml /stream description.
type PaymentEvent struct {
	PurchaseID      string         `json:"purchase_id"`
	AggregateType   string         `json:"aggregate_type"`
	State           string         `json:"state"`
	OccurredAt      string         `json:"occurred_at"`
	TenantID        string         `json:"tenant_id,omitempty"`
	LearnerGCID     string         `json:"learner_gcid,omitempty"`
	AmountCents     int64          `json:"amount_cents"`
	Currency        string         `json:"currency"`
	StripeSessionID string         `json:"stripe_session_id"`
	Metadata        map[string]any `json:"metadata,omitempty"`
}

// AdminHandler is the http.Handler entry point.
type AdminHandler struct {
	deps      AdminHandlerDeps
	marshaler protojson.MarshalOptions
}

// NewAdminHandler constructs an AdminHandler.
func NewAdminHandler(deps AdminHandlerDeps) *AdminHandler {
	if len(deps.AllowedRoles) == 0 {
		// Legacy roles kept for backward compatibility with existing
		// /api/v1/payments/* callers; the new /api/v1/admin/payments/*
		// routes go through ParseAdminRole (role_gate.go) which also
		// accepts the 4 canonical UPPER_SNAKE roles.
		deps.AllowedRoles = []string{
			"training-admin", "platform-admin",
			string(RoleTenantAdmin), string(RoleOwner),
			string(RoleAuditor), string(RolePlatformOperator),
		}
	}
	if deps.Now == nil {
		deps.Now = func() time.Time { return time.Now().UTC() }
	}
	return &AdminHandler{
		deps: deps,
		marshaler: protojson.MarshalOptions{
			UseProtoNames:   true,
			EmitUnpopulated: true,
		},
	}
}

// ServeHTTP routes /api/v1/payments/* (legacy) and /api/v1/admin/payments/*
// (H+ Transaction History) to the matching handler.
func (h *AdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := h.checkRole(r); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	// New /api/v1/admin/payments/* surface (ADR-165 / payments-admin.yaml).
	if strings.HasPrefix(r.URL.Path, "/api/v1/admin/payments/") {
		h.serveAdminPayments(w, r)
		return
	}
	// Legacy /api/v1/payments/* surface (pre-existing).
	if !strings.HasPrefix(r.URL.Path, "/api/v1/payments/") {
		writeError(w, http.StatusNotFound, "unknown path")
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/payments/")
	switch {
	case rest == "" || rest == "/":
		writeError(w, http.StatusNotFound, "purchase_id required in path")
	case strings.HasSuffix(rest, "/refund"):
		purchaseID := strings.TrimSuffix(rest, "/refund")
		h.handleRefund(w, r, purchaseID)
	default:
		h.handleGet(w, r, rest)
	}
}

// serveAdminPayments dispatches /api/v1/admin/payments/* to the 4 new
// H+ Transaction History handlers.
func (h *AdminHandler) serveAdminPayments(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/admin/payments")
	switch {
	case rest == "/purchases" || rest == "/purchases/":
		h.handleListPurchases(w, r)
	case rest == "/export" || rest == "/export/":
		h.handleExportPurchases(w, r)
	case rest == "/stream" || rest == "/stream/":
		h.handleStreamPaymentEvents(w, r)
	case strings.HasSuffix(rest, "/refund"):
		purchaseID := strings.TrimPrefix(strings.TrimSuffix(rest, "/refund"), "/")
		if purchaseID == "" {
			writeError(w, http.StatusBadRequest, "purchase_id required in path")
			return
		}
		h.handleAdminRefund(w, r, purchaseID)
	default:
		writeError(w, http.StatusNotFound, "unknown admin path: "+rest)
	}
}

func (h *AdminHandler) handleGet(w http.ResponseWriter, r *http.Request, purchaseID string) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	tenantID := resolveTenantID(r)
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "tenant_id required (X-Chora-Tenant-Id header or ?tenant_id=)")
		return
	}
	agg, err := parseAggregateType(r.URL.Query().Get("aggregate_type"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	req := &pb.GetPurchaseRequest{
		PurchaseId:    purchaseID,
		TenantId:      tenantID,
		AggregateType: agg,
	}
	resp, err := h.deps.Payments.GetPurchase(r.Context(), req)
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	h.writeProto(w, resp.Purchase)
}

func (h *AdminHandler) handleRefund(w http.ResponseWriter, r *http.Request, purchaseID string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	var body struct {
		TenantID      string `json:"tenant_id"`
		AggregateType string `json:"aggregate_type"`
		AmountCents   int64  `json:"amount_cents"`
		Reason        string `json:"reason"`
		Note          string `json:"note"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	tenantID := body.TenantID
	if tenantID == "" {
		tenantID = resolveTenantID(r)
	}
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "tenant_id required")
		return
	}
	agg, err := parseAggregateType(body.AggregateType)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	reason, err := parseRefundReason(body.Reason)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	req := &pb.RefundPurchaseRequest{
		PurchaseId:    purchaseID,
		TenantId:      tenantID,
		AggregateType: agg,
		AmountCents:   body.AmountCents,
		Reason:        reason,
		Note:          body.Note,
	}
	resp, err := h.deps.Payments.RefundPurchase(r.Context(), req)
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	h.writeProto(w, resp.Purchase)
}

func (h *AdminHandler) checkRole(r *http.Request) error {
	raw := r.Header.Get("X-Chora-Role")
	if raw == "" {
		return errors.New("X-Chora-Role missing (set by chora-gateway from ChoraSession)")
	}
	// chora-gateway stampRoleHeader comma-joins multi-role principals
	// (e.g. "TENANT_ADMIN,INSTRUCTOR"). Accept any matching role.
	for _, candidate := range strings.Split(raw, ",") {
		role := strings.TrimSpace(candidate)
		if role == "" {
			continue
		}
		for _, allowed := range h.deps.AllowedRoles {
			if role == allowed {
				return nil
			}
		}
	}
	return errors.New("none of roles [" + raw + "] allowed on /api/v1/payments/*")
}

func (h *AdminHandler) writeProto(w http.ResponseWriter, msg proto.Message) {
	bz, err := h.marshaler.Marshal(msg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "marshal: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(bz)
}

func resolveTenantID(r *http.Request) string {
	if v := r.Header.Get("X-Chora-Tenant-Id"); v != "" {
		return v
	}
	// chora-gateway stamps X-Tenant-Id (gatewayproxy stampAuthHeaders);
	// the canonical mesh header is lowercase chora-tenant-id. Accept both.
	if v := r.Header.Get("X-Tenant-Id"); v != "" {
		return v
	}
	if v := r.Header.Get("chora-tenant-id"); v != "" {
		return v
	}
	return r.URL.Query().Get("tenant_id")
}

func parseAggregateType(s string) (pb.AggregateType, error) {
	switch strings.ToLower(s) {
	case "course_purchase":
		return pb.AggregateType_AGGREGATE_TYPE_COURSE_PURCHASE, nil
	case "application_payment":
		return pb.AggregateType_AGGREGATE_TYPE_APPLICATION_PAYMENT, nil
	case "familiar_egg_purchase":
		return pb.AggregateType_AGGREGATE_TYPE_COMPANION_EGG_PURCHASE, nil
	case "tenant_mana_topup":
		return pb.AggregateType_AGGREGATE_TYPE_TENANT_MANA_TOPUP, nil
	case "user_subscription":
		return pb.AggregateType_AGGREGATE_TYPE_USER_SUBSCRIPTION, nil
	case "user_mana_topup":
		return pb.AggregateType_AGGREGATE_TYPE_USER_MANA_TOPUP, nil
	case "identity_kyc_fee":
		return pb.AggregateType_AGGREGATE_TYPE_IDENTITY_KYC_FEE, nil
	}
	return pb.AggregateType_AGGREGATE_TYPE_UNSPECIFIED, errors.New("aggregate_type must be one of course_purchase|application_payment|familiar_egg_purchase|tenant_mana_topup|user_subscription|user_mana_topup|identity_kyc_fee")
}

// parseFilterDate accepts either RFC3339 (e.g. "2026-05-26T00:00:00Z" — the
// canonical wire format) or a plain YYYY-MM-DD (what `<input type="date">`
// emits in the H+ FE filter chips). For YYYY-MM-DD: from-bound becomes
// 00:00:00 UTC, to-bound becomes 23:59:59.999999999 UTC so the inclusive
// human-meaning is preserved against the SQL `WHERE paid_at BETWEEN from
// AND to` shape.
func parseFilterDate(s string, isUpperBound bool) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		if isUpperBound {
			return t.Add(24*time.Hour - time.Nanosecond), nil
		}
		return t, nil
	}
	return time.Time{}, fmt.Errorf("must be RFC3339 or YYYY-MM-DD: %q", s)
}

func parseRefundReason(s string) (pb.RefundReason, error) {
	switch strings.ToLower(s) {
	case "", "customer_request":
		return pb.RefundReason_REFUND_REASON_CUSTOMER_REQUEST, nil
	case "support_initiated":
		return pb.RefundReason_REFUND_REASON_SUPPORT_INITIATED, nil
	case "duplicate_charge":
		return pb.RefundReason_REFUND_REASON_DUPLICATE_CHARGE, nil
	case "fraud":
		return pb.RefundReason_REFUND_REASON_FRAUD, nil
	case "expired_unhatched":
		return pb.RefundReason_REFUND_REASON_EXPIRED_UNHATCHED, nil
	}
	return pb.RefundReason_REFUND_REASON_UNSPECIFIED, errors.New("reason must be one of customer_request|support_initiated|duplicate_charge|fraud|expired_unhatched")
}

func writeGRPCError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	switch st.Code() {
	case codes.NotFound:
		writeError(w, http.StatusNotFound, st.Message())
	case codes.InvalidArgument:
		writeError(w, http.StatusBadRequest, st.Message())
	case codes.PermissionDenied:
		writeError(w, http.StatusForbidden, st.Message())
	case codes.FailedPrecondition:
		writeError(w, http.StatusConflict, st.Message())
	case codes.Unimplemented:
		writeError(w, http.StatusNotImplemented, st.Message())
	default:
		writeError(w, http.StatusInternalServerError, st.Message())
	}
}

// =============================================================================
// H+ Transaction History — 4 new admin endpoints per payments-admin.yaml
// =============================================================================

// listPurchasesEnvelope mirrors the OpenAPI ListPurchasesResponse shape.
type listPurchasesEnvelope struct {
	Items         []purchaseHistoryJSON `json:"items"`
	NextPageToken *string               `json:"next_page_token"`
	HasMore       bool                  `json:"has_more"`
}

// purchaseHistoryJSON mirrors the OpenAPI PurchaseHistoryItem shape.
type purchaseHistoryJSON struct {
	PurchaseID      string         `json:"purchase_id"`
	AggregateType   string         `json:"aggregate_type"`
	TenantID        string         `json:"tenant_id"`
	LearnerGCID     string         `json:"learner_gcid"`
	LearnerEmail    string         `json:"learner_email,omitempty"`
	AmountCents     int64          `json:"amount_cents"`
	Currency        string         `json:"currency"`
	State           string         `json:"state"`
	StripeSessionID string         `json:"stripe_session_id"`
	PaidAt          *string        `json:"paid_at"`
	RefundedAt      *string        `json:"refunded_at"`
	CreatedAt       string         `json:"created_at"`
	Metadata        map[string]any `json:"metadata,omitempty"`
}

// itemToJSON converts a domain PurchaseHistoryItem to the wire shape.
func itemToJSON(it payments.PurchaseHistoryItem) purchaseHistoryJSON {
	out := purchaseHistoryJSON{
		PurchaseID:      it.PurchaseID,
		AggregateType:   string(it.AggregateType),
		TenantID:        it.TenantID,
		LearnerGCID:     it.LearnerGCID,
		LearnerEmail:    it.LearnerEmail,
		AmountCents:     it.AmountCents,
		Currency:        it.Currency,
		State:           string(it.State),
		StripeSessionID: it.StripeSessionID,
		CreatedAt:       it.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
	if it.PaidAt != nil {
		s := it.PaidAt.UTC().Format(time.RFC3339Nano)
		out.PaidAt = &s
	}
	if it.RefundedAt != nil {
		s := it.RefundedAt.UTC().Format(time.RFC3339Nano)
		out.RefundedAt = &s
	}
	if it.MetadataJSON != "" {
		var m map[string]any
		if err := json.Unmarshal([]byte(it.MetadataJSON), &m); err == nil {
			out.Metadata = m
		}
	}
	return out
}

// parseFilter extracts the shared filter query params + validates them.
func (h *AdminHandler) parseFilter(r *http.Request, role AdminRole, callerTenantID string) (payments.FilterCriteria, error) {
	q := r.URL.Query()
	var f payments.FilterCriteria
	f.AggregateType = payments.AggregateType(strings.TrimSpace(q.Get("aggregate_type")))
	if state := strings.TrimSpace(q.Get("state")); state != "" {
		f.State = payments.AdminState(state)
	}
	if from := strings.TrimSpace(q.Get("from")); from != "" {
		t, err := parseFilterDate(from, false)
		if err != nil {
			return f, fmt.Errorf("invalid from: %w", err)
		}
		f.From = &t
	}
	if to := strings.TrimSpace(q.Get("to")); to != "" {
		t, err := parseFilterDate(to, true)
		if err != nil {
			return f, fmt.Errorf("invalid to: %w", err)
		}
		f.To = &t
	}
	// tenant_id is PLATFORM_OPERATOR-only narrowing. Silently dropped for
	// tenant-scoped callers — their scope is always the caller's tenant
	// (RLS-pinned).
	if role == RolePlatformOperator {
		f.TenantID = strings.TrimSpace(q.Get("tenant_id"))
	} else {
		f.TenantID = callerTenantID
	}
	return f, f.Validate()
}

// parsePageSize reads the page_size query param. Returns 0 → defaults
// to payments.PageSizeDefault inside the port.
func parsePageSize(q string) int {
	if q == "" {
		return 0
	}
	n, err := strconv.Atoi(q)
	if err != nil {
		return 0
	}
	return n
}

// handleListPurchases — GET /api/v1/admin/payments/purchases.
func (h *AdminHandler) handleListPurchases(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	if h.deps.History == nil {
		writeError(w, http.StatusNotImplemented, "purchase history port not wired")
		return
	}
	role, err := ParseAdminRole(r.Header.Get("X-Chora-Role"))
	if err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	tenantID := resolveTenantID(r)
	if role != RolePlatformOperator && tenantID == "" {
		writeError(w, http.StatusBadRequest, "tenant_id required (X-Chora-Tenant-Id header)")
		return
	}
	filter, err := h.parseFilter(r, role, tenantID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	pageSize := parsePageSize(r.URL.Query().Get("page_size"))
	cursor := r.URL.Query().Get("page_token")

	// Apply ctx flags. Tenant-scoped callers need tenant_id on ctx for RLS;
	// PLATFORM_OPERATOR gets the RLS bypass flag (purchase_history pg
	// adapter asserts on it before SELECT).
	ctx := r.Context()
	if role == RolePlatformOperator {
		ctx = ApplyOperatorContext(ctx, role)
	} else {
		ctx = tracing.WithTenantID(ctx, tenantID)
	}

	var (
		items      []payments.PurchaseHistoryItem
		nextCursor *string
		listErr    error
	)
	if role == RolePlatformOperator {
		items, nextCursor, listErr = h.deps.History.ListAll(ctx, filter, cursor, pageSize)
	} else {
		items, nextCursor, listErr = h.deps.History.ListByTenant(ctx, filter, cursor, pageSize)
	}
	if listErr != nil {
		writeError(w, http.StatusInternalServerError, listErr.Error())
		return
	}

	// Emit audit events (best-effort — outbox is fail-loud separately).
	actorGCID := r.Header.Get("X-Chora-Gcid")
	now := h.deps.Now()
	if h.deps.AuditOutbox != nil {
		_ = outbox.EmitTenantAdminViewedPayments(ctx, h.deps.AuditOutbox,
			tenantID, actorGCID, string(role), outboxFilter(filter), now)
		if role == RolePlatformOperator {
			tenantIDs := uniqueTenantIDs(items)
			_ = outbox.EmitCrossTenantPaymentsViewed(ctx, h.deps.AuditOutbox,
				actorGCID, string(role), outboxFilter(filter), tenantIDs, now)
		}
	}

	out := listPurchasesEnvelope{
		Items:         make([]purchaseHistoryJSON, 0, len(items)),
		NextPageToken: nextCursor,
		HasMore:       nextCursor != nil,
	}
	for _, it := range items {
		out.Items = append(out.Items, itemToJSON(it))
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(out)
}

// outboxFilter converts a domain FilterCriteria → outbox.PurchaseFilter
// (the audit-payload shape).
func outboxFilter(f payments.FilterCriteria) outbox.PurchaseFilter {
	return outbox.PurchaseFilter{
		AggregateType: string(f.AggregateType),
		State:         string(f.State),
		From:          f.From,
		To:            f.To,
	}
}

// uniqueTenantIDs returns the deduped + sorted set of tenant_ids in the
// items slice. Used by the cross_tenant_payments_viewed audit payload.
func uniqueTenantIDs(items []payments.PurchaseHistoryItem) []string {
	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
	for _, it := range items {
		if _, ok := seen[it.TenantID]; ok {
			continue
		}
		seen[it.TenantID] = struct{}{}
		out = append(out, it.TenantID)
	}
	return out
}

// handleAdminRefund — POST /api/v1/admin/payments/{purchase_id}/refund.
// Distinct from the legacy handleRefund: enforces the IsAuditor 403, pre-
// flights via the PurchaseHistoryPort (404 / 409 / found), routes the
// refund through the existing gRPC RefundPurchase RPC, and emits the
// refund_issued audit event.
func (h *AdminHandler) handleAdminRefund(w http.ResponseWriter, r *http.Request, purchaseID string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	role, err := ParseAdminRole(r.Header.Get("X-Chora-Role"))
	if err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	if !CanRefund(role) {
		writeError(w, http.StatusForbidden, "role "+string(role)+" cannot refund")
		return
	}

	var body struct {
		AggregateType string `json:"aggregate_type"`
		Reason        string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	agg, err := parseAggregateType(body.AggregateType)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	tenantID := resolveTenantID(r)
	if role != RolePlatformOperator && tenantID == "" {
		writeError(w, http.StatusBadRequest, "tenant_id required")
		return
	}

	// Pre-flight via PurchaseHistoryPort — 404 / 409 short-circuit
	// before we call Stripe.
	if h.deps.History != nil {
		preCtx := r.Context()
		if role == RolePlatformOperator {
			preCtx = ApplyOperatorContext(preCtx, role)
		} else {
			preCtx = tracing.WithTenantID(preCtx, tenantID)
		}
		row, lookupErr := h.deps.History.GetForRefund(preCtx, purchaseID, payments.AggregateType(strings.ToLower(body.AggregateType)))
		switch {
		case errors.Is(lookupErr, payments.ErrPurchaseNotFound):
			writeError(w, http.StatusNotFound, "purchase not found")
			return
		case errors.Is(lookupErr, payments.ErrAlreadyRefunded):
			writeError(w, http.StatusConflict, "purchase already refunded")
			return
		case lookupErr != nil:
			writeError(w, http.StatusInternalServerError, lookupErr.Error())
			return
		}
		// For PLATFORM_OPERATOR cross-tenant refunds, pin the discovered
		// tenant_id onto ctx so the gRPC RefundPurchase RPC's downstream
		// RLS uses the row's tenant.
		if role == RolePlatformOperator && row.TenantID != "" {
			tenantID = row.TenantID
		}
	}

	rpcReq := &pb.RefundPurchaseRequest{
		PurchaseId:    purchaseID,
		TenantId:      tenantID,
		AggregateType: agg,
		// AmountCents = 0 → full refund (per existing RefundPurchase RPC
		// convention).
		AmountCents: 0,
		Reason:      pb.RefundReason_REFUND_REASON_CUSTOMER_REQUEST,
		Note:        body.Reason,
	}
	rpcCtx := tracing.WithTenantID(r.Context(), tenantID)
	resp, err := h.deps.Payments.RefundPurchase(rpcCtx, rpcReq)
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	// Emit refund_issued audit event (best-effort).
	if h.deps.AuditOutbox != nil {
		_ = outbox.EmitRefundIssued(r.Context(), h.deps.AuditOutbox,
			tenantID,
			r.Header.Get("X-Chora-Gcid"),
			string(role),
			purchaseID,
			body.AggregateType,
			resp.GetPurchase().GetAmountCentsRefunded(),
			resp.GetPurchase().GetCurrency(),
			body.Reason,
			resp.GetPurchase().GetStripeRefundId(),
			h.deps.Now(),
		)
	}

	// Build a minimal refund response matching the OpenAPI shape.
	refundOut := map[string]any{
		"refund_id":    resp.GetPurchase().GetStripeRefundId(),
		"refunded_at":  protoTimeOrEmpty(resp.GetPurchase().GetRefundedAt().AsTime()),
		"amount_cents": resp.GetPurchase().GetAmountCentsRefunded(),
		"state":        string(payments.StateRefunded),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(refundOut)
}

// protoTimeOrEmpty formats a time.Time as RFC3339Nano or returns "" for
// the zero-value (which proto's AsTime() returns for unset timestamps).
func protoTimeOrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// handleExportPurchases — GET /api/v1/admin/payments/export.
// Streams the same filtered result set as ListPurchases without cursor
// pagination. CSV / NDJSON format selectable via ?format=csv|json.
//
// The stream loops in pages of payments.PageSizeMax to keep memory
// bounded — the full result set never lives in memory at once.
func (h *AdminHandler) handleExportPurchases(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	if h.deps.History == nil {
		writeError(w, http.StatusNotImplemented, "purchase history port not wired")
		return
	}
	role, err := ParseAdminRole(r.Header.Get("X-Chora-Role"))
	if err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	format := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("format")))
	if format != "csv" && format != "json" {
		writeError(w, http.StatusBadRequest, "format must be csv or json")
		return
	}
	tenantID := resolveTenantID(r)
	if role != RolePlatformOperator && tenantID == "" {
		writeError(w, http.StatusBadRequest, "tenant_id required")
		return
	}
	filter, err := h.parseFilter(r, role, tenantID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	if role == RolePlatformOperator {
		ctx = ApplyOperatorContext(ctx, role)
	} else {
		ctx = tracing.WithTenantID(ctx, tenantID)
	}

	// Set headers BEFORE first write so the browser prompts a download.
	stamp := h.deps.Now().Format("20060102-150405")
	ext := "csv"
	contentType := "text/csv; charset=utf-8"
	if format == "json" {
		ext = "ndjson"
		contentType = "application/x-ndjson; charset=utf-8"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="payments-export-%s.%s"`, stamp, ext))
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	if format == "csv" {
		h.writeCSVStream(w, flusher, ctx, role, filter)
	} else {
		h.writeNDJSONStream(w, flusher, ctx, role, filter)
	}
}

// writeCSVStream loops the PurchaseHistoryPort in PageSizeMax pages,
// writing each row as one CSV record + flushing.
func (h *AdminHandler) writeCSVStream(w http.ResponseWriter, flusher http.Flusher, ctx context.Context, role AdminRole, filter payments.FilterCriteria) {
	cw := csv.NewWriter(w)
	_ = cw.Write([]string{
		"purchase_id", "aggregate_type", "tenant_id", "learner_gcid",
		"learner_email", "amount_cents", "currency", "state",
		"stripe_session_id", "paid_at", "refunded_at", "created_at",
		"metadata_json",
	})
	cw.Flush()
	if flusher != nil {
		flusher.Flush()
	}

	cursor := ""
	for {
		items, next, err := h.callList(ctx, role, filter, cursor, payments.PageSizeMax)
		if err != nil {
			return
		}
		for _, it := range items {
			_ = cw.Write([]string{
				it.PurchaseID,
				string(it.AggregateType),
				it.TenantID,
				it.LearnerGCID,
				it.LearnerEmail,
				strconv.FormatInt(it.AmountCents, 10),
				it.Currency,
				string(it.State),
				it.StripeSessionID,
				ptrTimeStr(it.PaidAt),
				ptrTimeStr(it.RefundedAt),
				it.CreatedAt.UTC().Format(time.RFC3339Nano),
				it.MetadataJSON,
			})
		}
		cw.Flush()
		if flusher != nil {
			flusher.Flush()
		}
		if next == nil {
			return
		}
		cursor = *next
	}
}

// writeNDJSONStream loops the PurchaseHistoryPort + writes one
// JSON object per line + flushes after each page.
func (h *AdminHandler) writeNDJSONStream(w http.ResponseWriter, flusher http.Flusher, ctx context.Context, role AdminRole, filter payments.FilterCriteria) {
	enc := json.NewEncoder(w)
	cursor := ""
	for {
		items, next, err := h.callList(ctx, role, filter, cursor, payments.PageSizeMax)
		if err != nil {
			return
		}
		for _, it := range items {
			_ = enc.Encode(itemToJSON(it))
		}
		if flusher != nil {
			flusher.Flush()
		}
		if next == nil {
			return
		}
		cursor = *next
	}
}

func (h *AdminHandler) callList(ctx context.Context, role AdminRole, filter payments.FilterCriteria, cursor string, pageSize int) ([]payments.PurchaseHistoryItem, *string, error) {
	if role == RolePlatformOperator {
		return h.deps.History.ListAll(ctx, filter, cursor, pageSize)
	}
	return h.deps.History.ListByTenant(ctx, filter, cursor, pageSize)
}

func ptrTimeStr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// handleStreamPaymentEvents — GET /api/v1/admin/payments/stream (SSE).
//
// Opens a long-lived SSE connection. Each event is rendered as
//
//	event: payment_state_change
//	data: {…JSON…}
//
// followed by a blank line. Heartbeat comment lines (`:keepalive`) are
// emitted every 30s to keep proxy connections alive.
func (h *AdminHandler) handleStreamPaymentEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	role, err := ParseAdminRole(r.Header.Get("X-Chora-Role"))
	if err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	tenantID := resolveTenantID(r)
	if role != RolePlatformOperator && tenantID == "" {
		writeError(w, http.StatusBadRequest, "tenant_id required")
		return
	}
	if h.deps.PaymentEvents == nil {
		// Honour the SSE shape even when no source is wired: send a
		// single comment + 503 in a way the client can show in the UI.
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, ":payment_event_source not wired\n\n")
		return
	}

	aggregate := strings.TrimSpace(r.URL.Query().Get("aggregate_type"))
	scope := tenantID
	if role == RolePlatformOperator {
		// Operator gets cross-tenant fan-out — pass empty for "all".
		scope = strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	}

	ch, cleanup, err := h.deps.PaymentEvents.Subscribe(r.Context(), scope, aggregate)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "subscribe: "+err.Error())
		return
	}
	defer cleanup()

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}

	hb := time.NewTicker(30 * time.Second)
	defer hb.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-hb.C:
			_, _ = io.WriteString(w, ":keepalive\n\n")
			if flusher != nil {
				flusher.Flush()
			}
		case ev, ok := <-ch:
			if !ok {
				return
			}
			bz, _ := json.Marshal(ev)
			_, _ = fmt.Fprintf(w, "event: payment_state_change\ndata: %s\n\n", bz)
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}
