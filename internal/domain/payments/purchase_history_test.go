// purchase_history_test.go — domain-level shape tests for the cross-aggregate
// PurchaseHistoryItem projection + the FilterCriteria validation rules.
//
// Adapters (pg / in-mem) satisfy the PurchaseHistoryPort. This file only
// covers domain-pure invariants (no Postgres, no Stripe).
package payments

import (
	"testing"
	"time"
)

func TestPurchaseHistoryItem_RequiredFields(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	item := PurchaseHistoryItem{
		PurchaseID:      "01970000-0000-7000-b000-000000000003",
		AggregateType:   AggregateCoursePurchase,
		TenantID:        "01970000-0000-7000-8000-000000000001",
		LearnerGCID:     "01970000-0000-7000-a000-000000000002",
		AmountCents:     9900,
		Currency:        "sgd",
		State:           StateCaptured,
		StripeSessionID: "cs_test_abc",
		CreatedAt:       now,
	}
	if item.PurchaseID == "" {
		t.Error("PurchaseID empty")
	}
	if item.AggregateType != AggregateCoursePurchase {
		t.Errorf("AggregateType=%v want course_purchase", item.AggregateType)
	}
	if item.State != StateCaptured {
		t.Errorf("State=%v want captured", item.State)
	}
}

func TestFilterCriteria_Defaults(t *testing.T) {
	t.Parallel()
	f := FilterCriteria{}
	if f.AggregateType != AggregateUnspecified {
		t.Errorf("default AggregateType=%v want unspecified", f.AggregateType)
	}
	// Zero-value State is the empty string; adapter treats both ""
	// and StateAll as "no state narrowing".
	if f.State != AdminState("") {
		t.Errorf("default State=%v want empty (treated as all)", f.State)
	}
}

func TestFilterCriteria_Validate_RejectsInvalidState(t *testing.T) {
	t.Parallel()
	f := FilterCriteria{State: AdminState("nope")}
	if err := f.Validate(); err == nil {
		t.Errorf("Validate(state=nope) returned nil; want error")
	}
}

func TestFilterCriteria_Validate_AcceptsAllNormalisedStates(t *testing.T) {
	t.Parallel()
	for _, s := range []AdminState{StateCaptured, StateRefunded, StateFailed, StateExpired, StateAll, AdminState("")} {
		f := FilterCriteria{State: s}
		if err := f.Validate(); err != nil {
			t.Errorf("Validate(state=%q) returned %v; want nil", s, err)
		}
	}
}

func TestFilterCriteria_Validate_RejectsBackwardWindow(t *testing.T) {
	t.Parallel()
	from := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	f := FilterCriteria{From: &from, To: &to}
	if err := f.Validate(); err == nil {
		t.Errorf("Validate(from>to) returned nil; want error")
	}
}

func TestNormalisePageSize_ClampsZeroToDefault(t *testing.T) {
	t.Parallel()
	if got := NormalisePageSize(0); got != PageSizeDefault {
		t.Errorf("NormalisePageSize(0) = %d, want %d", got, PageSizeDefault)
	}
	if got := NormalisePageSize(-5); got != PageSizeDefault {
		t.Errorf("NormalisePageSize(-5) = %d, want %d", got, PageSizeDefault)
	}
}

func TestNormalisePageSize_CapsAtMax(t *testing.T) {
	t.Parallel()
	if got := NormalisePageSize(999); got != PageSizeMax {
		t.Errorf("NormalisePageSize(999) = %d, want %d", got, PageSizeMax)
	}
	if got := NormalisePageSize(PageSizeMax); got != PageSizeMax {
		t.Errorf("NormalisePageSize(max) = %d, want %d", got, PageSizeMax)
	}
}

func TestNormalisePageSize_PassesThroughInRange(t *testing.T) {
	t.Parallel()
	for _, n := range []int{1, 10, 20, 50, 99} {
		if got := NormalisePageSize(n); got != n {
			t.Errorf("NormalisePageSize(%d) = %d, want %d", n, got, n)
		}
	}
}

func TestAggregateType_InvalidIsRejected(t *testing.T) {
	t.Parallel()
	if AggregateType("nope").IsValid() {
		t.Error("AggregateType(nope).IsValid() = true; want false")
	}
}

func TestFilterCriteria_Validate_RejectsInvalidAggregate(t *testing.T) {
	t.Parallel()
	f := FilterCriteria{AggregateType: AggregateType("garbage")}
	if err := f.Validate(); err == nil {
		t.Error("Validate(invalid aggregate) returned nil; want error")
	}
}

func TestAdminStateFromShared_UnknownStateReturnsEmpty(t *testing.T) {
	t.Parallel()
	if got := AdminStateFromShared("bogus"); got != AdminState("") {
		t.Errorf("AdminStateFromShared(bogus) = %q; want empty", got)
	}
}

func TestAdminState_FromShared_Maps5StateFSMto4State(t *testing.T) {
	t.Parallel()
	cases := map[string]AdminState{
		"payment_captured": StateCaptured,
		"payment_failed":   StateFailed,
		"refunded":         StateRefunded,
		"expired":          StateExpired,
		"checkout_started": StateFailed, // collapsed per OpenAPI: in-flight surfaces as failed once it ages out
	}
	for src, want := range cases {
		got := AdminStateFromShared(src)
		if got != want {
			t.Errorf("AdminStateFromShared(%q) = %q; want %q", src, got, want)
		}
	}
}
