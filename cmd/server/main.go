// Package main wires the chora-payments Go service.
//
// Per ADR-164 chora-payments is the canonical Stripe-correlation home
// for the entire platform: 5 Purchase aggregates (course_purchase,
// application_payment, familiar_egg_purchase, tenant_mana_topup,
// user_subscription) + 1 inbound Stripe webhook receive path + 1
// outbound outbox-published event taxonomy (28 subjects) on the NATS
// JetStream event bus.
//
// This boot wires:
//   - HTTP listener :8080 with /healthz + /readyz + /v1/webhooks/stripe
//   - /api/v1/payments/* admin handler.
//   - gRPC listener :9090 with PaymentService (Create*Session × 5 +
//     GetPurchase + RefundPurchase) + health-check SERVING.
//   - Stripe SDK adapter (Stub when STRIPE_API_KEY unset; Real otherwise),
//     wired with Customer-reuse so Stripe persists saved cards.
//   - pg repos for all 5 Purchase aggregates + webhook_event dedup +
//     stripe_customer registry + outbox_events, when CHORA_DB_DSN_*
//     env is set; in-memory fallback otherwise.
//   - Real OutboxEmitter that writes to chora_payments.outbox_events,
//     and an OutboxDispatcher worker that drains the table to the
//     event bus. NoOpEmitter fallback when NATS_URL unset.
//
// Per `feedback_no_stubs_real_wiring` every gated path emits a clear
// WARNING in the boot log so the gap is visible at standalone smoke.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	nethttp "net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthgrpc "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/apollo-chora/chora-common/durabilityguard"
	"github.com/apollo-chora/chora-payments/internal/adapter/dispatcher"
	pgrpc "github.com/apollo-chora/chora-payments/internal/adapter/grpc"
	httpadapter "github.com/apollo-chora/chora-payments/internal/adapter/http"
	"github.com/apollo-chora/chora-payments/internal/adapter/outbox"
	"github.com/apollo-chora/chora-payments/internal/adapter/pubsub/event_broker"
	"github.com/apollo-chora/chora-payments/internal/adapter/repo/inmem"
	paymentspg "github.com/apollo-chora/chora-payments/internal/adapter/repo/pg"
	stripeadapter "github.com/apollo-chora/chora-payments/internal/adapter/stripe"

	apppay "github.com/apollo-chora/chora-payments/internal/domain/application_payment"
	"github.com/apollo-chora/chora-payments/internal/domain/coursepurchase"
	"github.com/apollo-chora/chora-payments/internal/domain/dispute"
	egg "github.com/apollo-chora/chora-payments/internal/domain/familiar_egg_purchase"
	kyc "github.com/apollo-chora/chora-payments/internal/domain/identity_kyc_fee"
	"github.com/apollo-chora/chora-payments/internal/domain/payments"
	stripecustomer "github.com/apollo-chora/chora-payments/internal/domain/stripe_customer"
	tap "github.com/apollo-chora/chora-payments/internal/domain/tenant_addon_purchase"
	mana "github.com/apollo-chora/chora-payments/internal/domain/tenant_mana_topup"
	umt "github.com/apollo-chora/chora-payments/internal/domain/user_mana_topup"
	sub "github.com/apollo-chora/chora-payments/internal/domain/user_subscription"
	wh "github.com/apollo-chora/chora-payments/internal/domain/webhook_event"
)

const (
	defaultHTTPPort = "8080"
	defaultGRPCPort = "9090"
)

var (
	// Set at build time via -ldflags.
	serviceName    = "chora-payments"
	serviceVersion = "0.1.0"
	gitSHA         = "unknown"
	buildTime      = "unknown"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	log.Printf("chora-payments: service=%s version=%s git=%s built=%s starting",
		serviceName, serviceVersion, gitSHA, buildTime)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	httpPort := envOrDefault("PORT", defaultHTTPPort)
	grpcPort := envOrDefault("CHORA_GRPC_PORT", envOrDefault("GRPC_PORT", defaultGRPCPort))

	// ----- Stripe SDK adapter wiring -------------------------------------
	stripeAPIKey := strings.TrimSpace(os.Getenv("STRIPE_API_KEY"))
	stripeTestMode := strings.EqualFold(os.Getenv("STRIPE_TEST_MODE"), "true")

	var stripeClient stripeadapter.Client
	if stripeAPIKey == "" {
		log.Printf("chora-payments: WARNING: STRIPE_API_KEY not set — using stub Stripe adapter (local-dev / standalone smoke only)")
		stripeClient = stripeadapter.NewStubClient()
	} else {
		rc, err := stripeadapter.NewRealClient(stripeAPIKey, stripeTestMode)
		if err != nil {
			log.Fatalf("chora-payments: NewRealClient: %v", err)
		}
		stripeClient = rc
		log.Printf("chora-payments: STRIPE_API_KEY loaded (%d chars, test_mode=%v) — Real Stripe adapter wired",
			len(stripeAPIKey), stripeTestMode)
	}

	// ----- Webhook secret check -----------------------------------------
	webhookSecret := strings.TrimSpace(os.Getenv("STRIPE_WEBHOOK_SECRET"))
	if webhookSecret == "" {
		log.Printf("chora-payments: WARNING: STRIPE_WEBHOOK_SECRET not set — /v1/webhooks/stripe will 503")
	} else {
		log.Printf("chora-payments: STRIPE_WEBHOOK_SECRET loaded (%d chars)", len(webhookSecret))
	}

	// ----- Stripe Price catalogue (CHO-1760) ----------------------------
	// Multi-env: STRIPE_PRICE_CATALOGUE_SECRET_ID points at a Secret
	// Manager secret holding the (addon:tier:currency → price_id) JSON
	// blob. Unset → placeholder catalogue (CHO-1762 + CHO-1764 refuse
	// to call Stripe with `price_TODO_` IDs).
	priceCatalogue, priceCatalogueShutdown := bootstrapPriceCatalogue(ctx)
	if priceCatalogueShutdown != nil {
		defer priceCatalogueShutdown()
	}
	// priceCatalogue is consumed by the gRPC create-checkout (CHO-1762)
	// + the HTTP change-tier handler (CHO-1764) below.

	// ----- pg pool + event bus ---------------------------------------
	pool, poolShutdown := bootstrapDBPool(ctx)
	if poolShutdown != nil {
		defer poolShutdown()
	}
	bus, busShutdown := bootstrapBus(ctx)
	if busShutdown != nil {
		defer busShutdown()
	}

	// ----- Repos: pg when pool wired, else in-memory --------------------
	var (
		courseRepo         coursepurchase.Repo
		appRepo            apppay.Repo
		eggRepo            egg.Repo
		manaRepo           mana.Repo
		subRepo            sub.Repo
		userManaRepo       umt.Repo
		kycFeeRepo         kyc.Repo
		addonPurchaseRepo  tap.Repo
		disputeRepo        dispute.Repo
		webhookEventsRepo  wh.Repo
		stripeCustomerRepo stripecustomer.Repo
		outboxRepo         *paymentspg.OutboxRepo
	)
	if txr := newPgxTxRunner(pool); txr != nil {
		courseRepo = paymentspg.NewCoursePurchaseRepo(txr)
		appRepo = paymentspg.NewApplicationPaymentRepo(txr)
		eggRepo = paymentspg.NewFamiliarEggPurchaseRepo(txr)
		manaRepo = paymentspg.NewTenantManaTopUpRepo(txr)
		subRepo = paymentspg.NewUserSubscriptionRepo(txr)
		userManaRepo = paymentspg.NewUserManaTopUpRepo(txr)
		kycFeeRepo = paymentspg.NewIdentityKycFeeRepo(txr)
		addonPurchaseRepo = paymentspg.NewTenantAddonPurchaseRepo(txr)
		disputeRepo = paymentspg.NewDisputeRepo(txr)
		webhookEventsRepo = paymentspg.NewWebhookEventRepo(txr)
		stripeCustomerRepo = paymentspg.NewStripeCustomerRepo(txr)
		outboxRepo = paymentspg.NewOutboxRepo(txr)
		log.Printf("chora-payments: pg repos wired (8 Purchase aggregates + dispute + webhook_event + stripe_customer + outbox)")
	} else {
		courseRepo = inmem.NewCoursePurchaseRepo()
		appRepo = inmem.NewApplicationPaymentRepo()
		eggRepo = inmem.NewFamiliarEggPurchaseRepo()
		manaRepo = inmem.NewTenantManaTopUpRepo()
		subRepo = inmem.NewUserSubscriptionRepo()
		userManaRepo = inmem.NewUserManaTopUpRepo()
		kycFeeRepo = inmem.NewIdentityKycFeeRepo()
		addonPurchaseRepo = inmem.NewTenantAddonPurchaseRepo()
		disputeRepo = inmem.NewDisputeRepo()
		webhookEventsRepo = inmem.NewWebhookEventRepo()
		stripeCustomerRepo = inmem.NewStripeCustomerRepo()
		log.Printf("chora-payments: in-memory repos wired (CHORA_DB_DSN unset; NOT durable across restart)")
	}

	// W0-F1 durability gate (CHO-2198): classify every wired repo by whether it
	// resolves to a durable adapter. Report-only unless CHORA_DURABILITY_GUARD=
	// enforce. chora-payments is a clean reference (all pg on a healthy pool).
	durabilityguard.Guard("chora-payments", []durabilityguard.Binding{
		{Port: "course_purchase", Adapter: courseRepo},
		{Port: "application_payment", Adapter: appRepo},
		{Port: "familiar_egg_purchase", Adapter: eggRepo},
		{Port: "tenant_mana_topup", Adapter: manaRepo},
		{Port: "user_subscription", Adapter: subRepo},
		{Port: "user_mana_topup", Adapter: userManaRepo},
		{Port: "identity_kyc_fee", Adapter: kycFeeRepo},
		{Port: "tenant_addon_purchase", Adapter: addonPurchaseRepo},
		{Port: "dispute", Adapter: disputeRepo},
		{Port: "webhook_event", Adapter: webhookEventsRepo},
		{Port: "stripe_customer", Adapter: stripeCustomerRepo},
	}, nil)

	// ----- Outbox emitter + dispatcher worker ---------------------------
	var outboxEmitter dispatcher.OutboxEmitter = dispatcher.NoOpEmitter{}
	if outboxRepo != nil && bus != nil {
		outboxEmitter = outbox.NewEmitter(outbox.EmitterDeps{
			Course:         courseRepo,
			Application:    appRepo,
			FamiliarEgg:    eggRepo,
			ManaTopUp:      manaRepo,
			Subscription:   subRepo,
			UserManaTopUp:  userManaRepo,
			IdentityKycFee: kycFeeRepo,
			AddonPurchase:  addonPurchaseRepo,
			Dispute:        disputeRepo,
			Outbox:         outboxRepo,
		})
		// Fail-loud preflight per feedback_resilience_priority +
		// feedback_no_stubs_real_wiring — a missing outbox_events table at
		// boot would otherwise spam-error every 250ms without surfacing
		// to liveness probes (Infra handoff a4-migration-blocker §4-#3).
		preflightCtx, preflightCancel := context.WithTimeout(ctx, 10*time.Second)
		if perr := outboxRepo.Preflight(preflightCtx); perr != nil {
			preflightCancel()
			log.Fatalf("chora-payments: outbox preflight failed — pod refuses to start: %v", perr)
		}
		preflightCancel()

		// Start the outbox-to-bus drain worker. The eventbus.Bus satisfies
		// outbox.Publisher directly (structurally identical) — no adapter.
		outboxDispatcher := outbox.NewDispatcher(outbox.DispatcherConfig{
			Outbox:    outboxRepo,
			Publisher: bus,
		})
		go func() {
			log.Printf("chora-payments: outbox dispatcher worker starting (worker_id=%s)", outboxWorkerID())
			if rerr := outboxDispatcher.Run(ctx); rerr != nil && !errors.Is(rerr, context.Canceled) {
				log.Printf("chora-payments: outbox dispatcher exited: %v", rerr)
			}
		}()
	} else {
		log.Printf("chora-payments: WARNING: outbox emitter degraded to NoOpEmitter (pg pool or event bus missing)")
	}

	// ----- Webhook dispatcher -------------------------------------------
	disp := dispatcher.New(dispatcher.Deps{
		Course:         courseRepo,
		Application:    appRepo,
		FamiliarEgg:    eggRepo,
		ManaTopUp:      manaRepo,
		Subscription:   subRepo,
		UserManaTopUp:  userManaRepo,
		IdentityKycFee: kycFeeRepo,
		AddonPurchase:  addonPurchaseRepo,
		Dispute:        disputeRepo,
		Outbox:         outboxEmitter,
	})

	// ----- gRPC PaymentService ------------------------------------------
	defaultSuccessURL := envOrDefault("STRIPE_DEFAULT_SUCCESS_URL", "https://chora.site/payment/success")
	defaultCancelURL := envOrDefault("STRIPE_DEFAULT_CANCEL_URL", "https://chora.site/payment/cancel")
	paymentSvc := pgrpc.New(pgrpc.Deps{
		Course:          courseRepo,
		Application:     appRepo,
		FamiliarEgg:     eggRepo,
		ManaTopUp:       manaRepo,
		Subscription:    subRepo,
		UserManaTopUp:   userManaRepo,
		IdentityKycFee:  kycFeeRepo,
		AddonPurchase:   addonPurchaseRepo,
		StripeCustomers: stripeCustomerRepo,
		Stripe:          stripeClient,
		// CHO-1762 — Stripe Price catalogue resolved at boot (CHO-1760).
		// Used by CreateTenantAddonCheckoutSession to switch the Stripe
		// Checkout from mode=payment to mode=subscription with a real
		// recurring Price ID. nil-safe — falls back to mode=payment.
		PriceCatalogue:    priceCatalogue,
		DefaultSuccessURL: defaultSuccessURL,
		DefaultCancelURL:  defaultCancelURL,
		// Wire the outbox emitter via an adapter so CancelSubscription
		// can publish chora.payments.user_subscription.cancelled.v1.
		Outbox: grpcOutboxAdapter{emit: outboxEmitter},
	})

	// ----- HTTP server --------------------------------------------------
	mux := nethttp.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler)
	mux.HandleFunc("/healthz/", healthHandler)
	mux.HandleFunc("/readyz", readyHandler)
	mux.HandleFunc("/readyz/", readyHandler)
	mux.HandleFunc("/v1/webhooks/stripe", httpadapter.NewWebhookHandler(httpadapter.WebhookDeps{
		WebhookSecret: webhookSecret,
		WebhookEvents: webhookEventsRepo,
		Dispatcher:    disp,
	}))
	// ----- H+ Transaction History wiring (ADR-165) ----------------------
	// PurchaseHistoryRepo backs the 4 new /api/v1/admin/payments/* routes
	// (list / refund / export / stream). Available only when pg is wired —
	// in-memory shim is a follow-up.
	var purchaseHistory payments.PurchaseHistoryPort
	if txr := newPgxTxRunner(pool); txr != nil && stripeCustomerRepo != nil {
		// Cast the stripe-customer repo (interface) back to concrete pg
		// type so the history adapter can use the same TxRunner+JOIN.
		if pgSC, ok := stripeCustomerRepo.(*paymentspg.StripeCustomerRepo); ok {
			purchaseHistory = paymentspg.NewPurchaseHistoryRepo(txr, pgSC)
			log.Printf("chora-payments: PurchaseHistoryPort wired (H+ Transaction History admin)")
		}
	}
	// AuditOutbox adapter for the 3 governance.audit.* events. Skipped
	// when pg outbox isn't wired — audit emit becomes a no-op in that
	// case (fail-open by design; admin reads continue working).
	var auditOutbox outbox.AuditOutbox
	if outboxRepo != nil {
		auditOutbox = &outbox.AuditOutboxAdapter{Outbox: outboxRepo}
	}

	// ----- A1.1 — event-bus fan-out broker for SSE /stream --------------
	// Per the broker design (shared durable consumer per subject;
	// in-memory per-HTTP-client channels) — see
	// internal/adapter/pubsub/event_broker/broker.go. Skipped when the
	// bus is missing — SSE /stream falls back to the 503 path the
	// existing handler already implements.
	var paymentEventBroker httpadapter.PaymentEventSource
	if bus != nil {
		broker, brokerErr := event_broker.NewBroker(event_broker.BrokerConfig{
			Bus:                    bus,
			SubscriptionsBySubject: paymentBrokerSubscriptionMap(),
		})
		if brokerErr != nil {
			log.Fatalf("chora-payments: event_broker init: %v", brokerErr)
		}
		// Wrap the concrete broker as a PaymentEventSource that
		// converts the broker's PaymentEvent struct into the http
		// adapter's identical-shape struct (Go forbids cross-package
		// type assertions; the adapter's a 1-field copy).
		paymentEventBroker = &paymentEventBrokerAdapter{broker: broker}
		go func() {
			log.Printf("chora-payments: event_broker starting (28 subjects × durable consumers)")
			if rerr := broker.Run(ctx); rerr != nil && !errors.Is(rerr, context.Canceled) {
				log.Printf("chora-payments: event_broker exited: %v", rerr)
			}
		}()
	} else {
		log.Printf("chora-payments: WARNING: event_broker NOT wired (NATS_URL unset) — SSE /stream will 503")
	}

	adminHandler := httpadapter.NewAdminHandler(httpadapter.AdminHandlerDeps{
		Payments:      paymentSvc,
		History:       purchaseHistory,
		AuditOutbox:   auditOutbox,
		PaymentEvents: paymentEventBroker,
	})
	mux.Handle("/api/v1/payments/", adminHandler)
	mux.Handle("/api/v1/admin/payments/", adminHandler)

	// CHO-1764 — H+ Marketplace change-tier endpoint (chora-tenancy
	// calls here from handleAdminChangeAddonTier; refactor in CHO-1764b).
	// Mounted independently so the existing /admin/payments/ rolegate
	// stays unchanged; this endpoint trusts network (gateway verified
	// the originating admin token).
	mux.Handle("/api/v1/admin/tenant-addons:change-tier", httpadapter.NewChangeTierHandler(httpadapter.ChangeTierHandlerDeps{
		AddonPurchase:  addonPurchaseRepo,
		Stripe:         stripeClient,
		PriceCatalogue: priceCatalogue,
	}))

	// CHO-1765 — H+ Marketplace preview-tier-change endpoint. Read-only
	// dispatch through chora-tenancy's existing handleAdminPreviewTierChange
	// (CHO-1767 refactor). Returns the real Stripe Invoice.upcoming
	// proration delta instead of the catalogue-estimated stub.
	mux.Handle("/api/v1/admin/tenant-addons:preview-tier-change", httpadapter.NewPreviewTierHandler(httpadapter.PreviewTierHandlerDeps{
		AddonPurchase:  addonPurchaseRepo,
		Stripe:         stripeClient,
		PriceCatalogue: priceCatalogue,
	}))

	// H+ Billing (CHO-1759 followup) — tenant-scoped invoice list +
	// Stripe Customer Portal session mint. Both mount under the single
	// `/api/v1/admin/tenants/` prefix; the dispatcher routes by the
	// subresource suffix (`/invoices` or `/billing-portal`). The leaf
	// handlers resolve the tenant's Stripe Customer via the TAP repo
	// (CHO-1762 contract: one Customer per tenant).
	mux.Handle("/api/v1/admin/tenants/", httpadapter.NewAdminTenantsRouter(
		httpadapter.NewAdminInvoicesHandler(httpadapter.AdminInvoicesHandlerDeps{
			AddonPurchase: addonPurchaseRepo,
			Stripe:        stripeClient,
		}),
		httpadapter.NewAdminBillingPortalHandler(httpadapter.AdminBillingPortalHandlerDeps{
			AddonPurchase: addonPurchaseRepo,
			Stripe:        stripeClient,
		}),
	))

	httpSrv := &nethttp.Server{
		Addr:              ":" + httpPort,
		Handler:           loggingMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	// ----- gRPC server --------------------------------------------------
	grpcLis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("chora-payments: gRPC listen :%s: %v", grpcPort, err)
	}
	grpcSrv := grpc.NewServer(
		grpc.Creds(insecure.NewCredentials()),
		// ADR-188 D3 — chora-payments' gRPC handler errors were logged
		// nowhere (only the HTTP middleware logs), so a failing
		// Create*Session Save() returned Internal to the gateway and was
		// invisible here. Surface every handler error loud.
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			resp, err := handler(ctx, req)
			if err != nil {
				log.Printf("chora-payments: gRPC %s failed: code=%s err=%v", info.FullMethod, status.Code(err), err)
			}
			return resp, err
		}),
	)
	// ADR-254 D9 W4-to-W5 overlap: registers PaymentService ONCE under a
	// descriptor that answers the egg checkout under both its renamed and its
	// pre-rename method name. Replaces pb.RegisterPaymentServiceServer; calling
	// both would panic on a duplicate service registration. chora-gateway rolls
	// at W4 and chora-tenancy at W5, so without this the roll would simply move
	// the outage from one caller to the other. See grpc_legacy_alias.go.
	registerPaymentServiceWithLegacyEggAlias(grpcSrv, paymentSvc)
	healthSrv := healthgrpc.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus("chora.payments.v1.PaymentService", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcSrv, healthSrv)

	errCh := make(chan error, 2)
	go func() {
		log.Printf("chora-payments: HTTP listener :%s up", httpPort)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, nethttp.ErrServerClosed) {
			errCh <- fmt.Errorf("http: %w", err)
		}
	}()
	go func() {
		log.Printf("chora-payments: gRPC listener :%s up", grpcPort)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			errCh <- fmt.Errorf("grpc: %w", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		log.Printf("chora-payments: signal=%s — initiating graceful shutdown", sig)
	case err := <-errCh:
		log.Printf("chora-payments: listener error: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	grpcSrv.GracefulStop()
	log.Printf("chora-payments: shutdown complete")
}

func envOrDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func healthHandler(w nethttp.ResponseWriter, _ *nethttp.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(nethttp.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":  "ok",
		"service": serviceName,
		"version": serviceVersion,
		"git":     gitSHA,
	})
}

func readyHandler(w nethttp.ResponseWriter, r *nethttp.Request) {
	healthHandler(w, r)
}

func loggingMiddleware(next nethttp.Handler) nethttp.Handler {
	return nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		log.Printf("service=%s method=%s path=%s status=%d duration_ms=%d",
			serviceName, r.Method, r.URL.Path, sw.status, time.Since(start).Milliseconds())
	})
}

type statusWriter struct {
	nethttp.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = nethttp.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// grpcOutboxAdapter satisfies pgrpc.OutboxEmitter by delegating to the
// dispatcher.OutboxEmitter (outbox.Emitter or NoOpEmitter). The two ports
// differ only in the EmitInput struct shape; this adapter performs the
// trivial copy. Per Stage A.5 — closes the FE-initiated cancel-event gap.
type grpcOutboxAdapter struct {
	emit dispatcher.OutboxEmitter
}

func (a grpcOutboxAdapter) Emit(ctx context.Context, in pgrpc.OutboxEmitInput) error {
	if a.emit == nil {
		return nil
	}
	return a.emit.Emit(ctx, dispatcher.EmitInput{
		TenantID:       in.TenantID,
		Aggregate:      in.Aggregate,
		PurchaseID:     in.PurchaseID,
		EventType:      dispatcher.EventType(in.EventType),
		IdempotencyKey: in.IdempotencyKey,
	})
}

// =============================================================================
// A1.1 — SSE event broker wiring helpers (appended for parallel-session
// coordination safety per [[feedback-check-parallel-session-state]]).
// =============================================================================

// paymentBrokerSubscriptionMap builds the (subject → consumer) map the
// PaymentEventBroker uses. Each entry can be overridden via a per-subject
// env var; the default uses event_broker.DefaultSubscriptionName.
//
// Per-subject env-var pattern:
//
//	CHORA_PAYMENTS_SSE_SUB__<aggregate>_<event_type>
//	    e.g. CHORA_PAYMENTS_SSE_SUB__COURSE_PURCHASE_PAYMENT_CAPTURED
//
// (Uppercase + double-underscore between segment groups so it's unambiguous
// against shell glob expansion.) The map keys MUST match
// event_broker.SupportedSubjects().
//
// Consumer provisioning is a deployment prereq — the eventbus sanitises
// the durable consumer name to a NATS-legal value internally.
func paymentBrokerSubscriptionMap() map[string]string {
	defaults := event_broker.DefaultSubscriptionMap()
	out := make(map[string]string, len(defaults))
	for subject, defaultSub := range defaults {
		envKey := paymentSseSubEnvKey(subject)
		if v := strings.TrimSpace(os.Getenv(envKey)); v != "" {
			out[subject] = v
		} else {
			out[subject] = defaultSub
		}
	}
	return out
}

// paymentSseSubEnvKey converts a chora.payments.{agg}.{ev}.v1 subject name
// into the per-subject env-var key.
func paymentSseSubEnvKey(subject string) string {
	// strip "chora.payments." prefix + ".v1" suffix → "{agg}.{ev}"
	rest := strings.TrimPrefix(subject, "chora.payments.")
	parts := strings.Split(rest, ".")
	if len(parts) < 3 {
		return ""
	}
	core := strings.Join(parts[:len(parts)-1], "_") // drop "v1"
	return "CHORA_PAYMENTS_SSE_SUB__" + strings.ToUpper(core)
}

// paymentEventBrokerAdapter bridges the concrete *event_broker.Broker to the
// httpadapter.PaymentEventSource port. The two PaymentEvent struct shapes
// are identical-by-field so this is a one-field copy.
type paymentEventBrokerAdapter struct {
	broker *event_broker.Broker
}

func (a *paymentEventBrokerAdapter) Subscribe(ctx context.Context, tenantID, aggregateType string) (<-chan httpadapter.PaymentEvent, func(), error) {
	in, cleanup, err := a.broker.Subscribe(ctx, tenantID, aggregateType)
	if err != nil {
		return nil, func() {}, err
	}
	out := make(chan httpadapter.PaymentEvent, cap(in))
	go func() {
		defer close(out)
		for ev := range in {
			out <- httpadapter.PaymentEvent{
				PurchaseID:      ev.PurchaseID,
				AggregateType:   ev.AggregateType,
				State:           ev.State,
				OccurredAt:      ev.OccurredAt,
				TenantID:        ev.TenantID,
				LearnerGCID:     ev.LearnerGCID,
				AmountCents:     ev.AmountCents,
				Currency:        ev.Currency,
				StripeSessionID: ev.StripeSessionID,
			}
		}
	}()
	return out, cleanup, nil
}
