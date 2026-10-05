// client_real_change_tier_delta_test.go — CHO-1789.
//
// Pins the proration-behavior → billing-delta source mapping for the
// immediate change-tier path. The bug it captures: with
// proration_behavior=always_invoice Stripe finalises a SEPARATE invoice
// today, so the next-cycle invoice.Upcoming has zero proration lines and
// the previous logic returned delta=0 — the FE result card then showed
// "$0.00" even though the customer was billed (or credited) immediately.
package stripe

import "testing"

func TestComputeBillingDelta_AlwaysInvoice_ReadsLatestInvoiceTotal(t *testing.T) {
	got := computeBillingDelta("always_invoice", 9999 /* latest_invoice.total */, 0 /* upcoming proration sum */)
	if got != 9999 {
		t.Fatalf("always_invoice should surface latest_invoice.total (the just-issued subscription_update invoice); got %d want 9999", got)
	}
}

func TestComputeBillingDelta_AlwaysInvoice_NegativeIsCredit(t *testing.T) {
	got := computeBillingDelta("always_invoice", -3410, 0)
	if got != -3410 {
		t.Fatalf("downgrade credit must propagate as a negative delta; got %d want -3410", got)
	}
}

func TestComputeBillingDelta_CreateProrations_ReadsUpcomingProration(t *testing.T) {
	got := computeBillingDelta("create_prorations", 0, 7000)
	if got != 7000 {
		t.Fatalf("create_prorations should sum proration lines from invoice.Upcoming; got %d want 7000", got)
	}
}

func TestComputeBillingDelta_EmptyDefaultsToCreateProrations(t *testing.T) {
	got := computeBillingDelta("", 0, 1234)
	if got != 1234 {
		t.Fatalf("empty proration_behavior should default to create_prorations path; got %d want 1234", got)
	}
}

func TestComputeBillingDelta_None_ReturnsZero(t *testing.T) {
	got := computeBillingDelta("none", 9999, 7000)
	if got != 0 {
		t.Fatalf("proration_behavior=none must report zero delta regardless of invoice totals; got %d", got)
	}
}
