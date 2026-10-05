// Package stripe_catalogue_test — CHO-1760.
//
// Strict TDD: RED tests first, then GREEN implementation. Per
// chora/.claude/rules/development-execution.md.
package stripe_catalogue_test

import (
	"context"
	"errors"
	"testing"

	stripe_catalogue "github.com/apollo-chora/chora-payments/internal/adapter/stripe_catalogue"
)

// -----------------------------------------------------------------------------
// StaticPriceCatalogue
// -----------------------------------------------------------------------------

func TestStaticPriceCatalogue_Resolve_HitsKnownPair(t *testing.T) {
	t.Parallel()
	cat := stripe_catalogue.NewStaticPriceCatalogue(map[stripe_catalogue.Key]string{
		{AddonCode: "tms", TierCode: "pro", Currency: "usd"}: "price_abc",
	})
	got, ok := cat.Resolve("tms", "pro", "usd")
	if !ok {
		t.Fatalf("expected ok=true; got false")
	}
	if got != "price_abc" {
		t.Fatalf("expected price_abc, got %q", got)
	}
}

func TestStaticPriceCatalogue_Resolve_MissesOnUnknownAddon(t *testing.T) {
	t.Parallel()
	cat := stripe_catalogue.NewStaticPriceCatalogue(map[stripe_catalogue.Key]string{
		{AddonCode: "tms", TierCode: "pro", Currency: "usd"}: "price_abc",
	})
	if _, ok := cat.Resolve("unknown_addon", "pro", "usd"); ok {
		t.Fatalf("expected ok=false on unknown addon; got true")
	}
}

func TestStaticPriceCatalogue_Resolve_MissesOnUnknownTier(t *testing.T) {
	t.Parallel()
	cat := stripe_catalogue.NewStaticPriceCatalogue(map[stripe_catalogue.Key]string{
		{AddonCode: "tms", TierCode: "pro", Currency: "usd"}: "price_abc",
	})
	if _, ok := cat.Resolve("tms", "enterprise", "usd"); ok {
		t.Fatalf("expected ok=false on unknown tier; got true")
	}
}

func TestStaticPriceCatalogue_Resolve_MissesOnWrongCurrency(t *testing.T) {
	t.Parallel()
	cat := stripe_catalogue.NewStaticPriceCatalogue(map[stripe_catalogue.Key]string{
		{AddonCode: "tms", TierCode: "pro", Currency: "usd"}: "price_abc",
	})
	if _, ok := cat.Resolve("tms", "pro", "sgd"); ok {
		t.Fatalf("expected ok=false on wrong currency; got true")
	}
}

// Stripe wire currency codes are lowercase. Callers may pass mixed case
// (FE seed catalogue uses "USD"). The catalogue normalises on read +
// write so the lookup matches.
func TestStaticPriceCatalogue_Resolve_NormalisesCurrencyCase(t *testing.T) {
	t.Parallel()
	cat := stripe_catalogue.NewStaticPriceCatalogue(map[stripe_catalogue.Key]string{
		{AddonCode: "tms", TierCode: "pro", Currency: "USD"}: "price_abc",
	})
	got, ok := cat.Resolve("tms", "pro", "usd")
	if !ok || got != "price_abc" {
		t.Fatalf("expected price_abc on lowercase lookup of upper-cased key; got (%q, %v)", got, ok)
	}
	got, ok = cat.Resolve("tms", "pro", "USD")
	if !ok || got != "price_abc" {
		t.Fatalf("expected price_abc on mixed-case lookup; got (%q, %v)", got, ok)
	}
}

// -----------------------------------------------------------------------------
// PlaceholderPriceCatalogue
// -----------------------------------------------------------------------------

// Placeholders always resolve. CHO-1762 + CHO-1764 are responsible for
// detecting the `price_TODO_` prefix via IsPlaceholder + refusing to
// call Stripe with one — placeholder catalogue itself does not gate.
func TestPlaceholderPriceCatalogue_Resolve_AlwaysReturnsTODOPrefix(t *testing.T) {
	t.Parallel()
	cat := stripe_catalogue.NewPlaceholderPriceCatalogue()
	got, ok := cat.Resolve("any_addon", "any_tier", "USD")
	if !ok {
		t.Fatalf("expected ok=true; got false")
	}
	want := "price_TODO_any_addon_any_tier_usd"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestIsPlaceholder_MatchesPrefix(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{
		"price_TODO_tms_pro_usd": true,
		"price_TODO_":            true,
		"price_abc":              false,
		"":                       false,
		"TODO_price_tms":         false,
	}
	for in, want := range cases {
		if got := stripe_catalogue.IsPlaceholder(in); got != want {
			t.Errorf("IsPlaceholder(%q): want %v, got %v", in, want, got)
		}
	}
}

// -----------------------------------------------------------------------------
// EnvPriceCatalogue
// -----------------------------------------------------------------------------

// stubFetcher implements stripe_catalogue.SecretFetcher for tests.
type stubFetcher struct {
	values map[string]string
	err    error
	calls  int
}

func (s *stubFetcher) GetSecret(_ context.Context, name string) (string, error) {
	s.calls++
	if s.err != nil {
		return "", s.err
	}
	v, ok := s.values[name]
	if !ok {
		return "", errSecretNotFound
	}
	return v, nil
}

var errSecretNotFound = errors.New("test: secret not found")

func TestEnvPriceCatalogue_LoadsJsonOnConstruction(t *testing.T) {
	t.Parallel()
	fetcher := &stubFetcher{values: map[string]string{
		"chora-stripe-addon-prices-dev": `{"tms:pro:usd":"price_xyz"}`,
	}}
	cat, err := stripe_catalogue.NewEnvPriceCatalogue(
		context.Background(), fetcher, "chora-stripe-addon-prices-dev",
	)
	if err != nil {
		t.Fatalf("construction: %v", err)
	}
	got, ok := cat.Resolve("tms", "pro", "usd")
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if got != "price_xyz" {
		t.Fatalf("expected price_xyz, got %q", got)
	}
}

// Once constructed, Resolve hits the in-memory map. The secret store is
// not re-fetched per call.
func TestEnvPriceCatalogue_CachesAfterFirstLoad(t *testing.T) {
	t.Parallel()
	fetcher := &stubFetcher{values: map[string]string{
		"chora-stripe-addon-prices-dev": `{"tms:pro:usd":"price_xyz","tms:starter:usd":"price_abc"}`,
	}}
	cat, err := stripe_catalogue.NewEnvPriceCatalogue(
		context.Background(), fetcher, "chora-stripe-addon-prices-dev",
	)
	if err != nil {
		t.Fatalf("construction: %v", err)
	}
	for i := 0; i < 10; i++ {
		_, _ = cat.Resolve("tms", "pro", "usd")
		_, _ = cat.Resolve("tms", "starter", "usd")
	}
	if fetcher.calls != 1 {
		t.Fatalf("expected fetcher.GetSecret called exactly once (at construction); got %d", fetcher.calls)
	}
}

func TestEnvPriceCatalogue_RejectsMalformedJSON(t *testing.T) {
	t.Parallel()
	fetcher := &stubFetcher{values: map[string]string{
		"bad": `{not json`,
	}}
	if _, err := stripe_catalogue.NewEnvPriceCatalogue(
		context.Background(), fetcher, "bad",
	); err == nil {
		t.Fatalf("expected error on malformed JSON; got nil")
	}
}

// Keys must be in `<addon>:<tier>:<currency>` form — anything else
// fails-loud at construction (preventing silent typo bugs).
func TestEnvPriceCatalogue_RejectsMalformedKey(t *testing.T) {
	t.Parallel()
	fetcher := &stubFetcher{values: map[string]string{
		"bad": `{"tms_pro_usd":"price_xyz"}`, // underscore instead of colon
	}}
	if _, err := stripe_catalogue.NewEnvPriceCatalogue(
		context.Background(), fetcher, "bad",
	); err == nil {
		t.Fatalf("expected error on malformed key; got nil")
	}
}

// Missing tuple after a successful load returns (empty, false) — same
// contract as StaticPriceCatalogue.
func TestEnvPriceCatalogue_MissingTupleReturnsOkFalse(t *testing.T) {
	t.Parallel()
	fetcher := &stubFetcher{values: map[string]string{
		"prices": `{"tms:pro:usd":"price_xyz"}`,
	}}
	cat, err := stripe_catalogue.NewEnvPriceCatalogue(
		context.Background(), fetcher, "prices",
	)
	if err != nil {
		t.Fatalf("construction: %v", err)
	}
	if _, ok := cat.Resolve("tms", "starter", "usd"); ok {
		t.Fatalf("expected ok=false on missing tuple; got true")
	}
}

// Underlying fetcher error bubbles up so main.go can fail-loud at boot.
func TestEnvPriceCatalogue_FetcherErrorBubblesUp(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("test: fetcher unavailable")
	fetcher := &stubFetcher{err: wantErr}
	_, err := stripe_catalogue.NewEnvPriceCatalogue(
		context.Background(), fetcher, "anything",
	)
	if err == nil {
		t.Fatalf("expected error; got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrap of %v; got %v", wantErr, err)
	}
}

func TestEnvPriceCatalogue_RejectsNilFetcher(t *testing.T) {
	t.Parallel()
	if _, err := stripe_catalogue.NewEnvPriceCatalogue(
		context.Background(), nil, "x",
	); err == nil {
		t.Fatalf("expected error on nil fetcher; got nil")
	}
}

func TestEnvPriceCatalogue_RejectsEmptySecretID(t *testing.T) {
	t.Parallel()
	if _, err := stripe_catalogue.NewEnvPriceCatalogue(
		context.Background(), &stubFetcher{}, "",
	); err == nil {
		t.Fatalf("expected error on empty secretID; got nil")
	}
}

// Compile-time contract check — every catalogue impl satisfies the
// interface. If this stops compiling, an impl drifted.
func TestInterfaceCompliance(t *testing.T) {
	t.Parallel()
	var _ stripe_catalogue.PriceCatalogue = (*stripe_catalogue.StaticPriceCatalogue)(nil)
	var _ stripe_catalogue.PriceCatalogue = (*stripe_catalogue.PlaceholderPriceCatalogue)(nil)
}
