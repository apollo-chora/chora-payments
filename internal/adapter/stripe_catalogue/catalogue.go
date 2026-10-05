// Package stripe_catalogue resolves (addon_code, tier_code, currency)
// tuples to Stripe Price IDs (recurring monthly Prices created in the
// targeted Stripe sandbox / production account).
//
// CHO-1759 / CHO-1760. Multi-environment design — config flows from
// the env-backed secret resolver per [[feedback_no_inline_config]]. Same compiled
// binary serves local dev (App4U sandbox) + prod demo (separate
// sandbox); only the STRIPE_PRICE_CATALOGUE_SECRET_ID env var differs.
//
// Wiring:
//
//	main.go reads STRIPE_PRICE_CATALOGUE_SECRET_ID. When set,
//	NewEnvPriceCatalogue eager-loads the JSON blob at boot.
//	When unset (unit tests / fresh dev box), NewPlaceholderPriceCatalogue
//	returns `price_TODO_<addon>_<tier>_<currency>` for any lookup;
//	downstream Stripe-call sites (CHO-1762, CHO-1764) use IsPlaceholder
//	to refuse + 422.
package stripe_catalogue

import (
	"fmt"
	"strings"
)

// PriceCatalogue resolves an (addon, tier, currency) tuple to a Stripe
// Price ID (recurring monthly). Implementations are immutable once
// constructed; rebuild + restart to pick up Stripe catalogue changes.
type PriceCatalogue interface {
	Resolve(addonCode, tierCode, currency string) (stripePriceID string, ok bool)
}

// Key is the catalogue lookup key. Currency is normalised to lowercase
// on insert + lookup so callers can pass either form.
type Key struct {
	AddonCode string
	TierCode  string
	Currency  string
}

func (k Key) normalised() Key {
	return Key{
		AddonCode: k.AddonCode,
		TierCode:  k.TierCode,
		Currency:  strings.ToLower(k.Currency),
	}
}

// StaticPriceCatalogue is an in-memory map implementation. Used as the
// post-load cache for EnvPriceCatalogue + as a test helper.
type StaticPriceCatalogue struct {
	m map[Key]string
}

// NewStaticPriceCatalogue copies the entries into an internal map so
// later mutations on the caller's map don't affect the catalogue.
// Currency portion of every key is lowercased.
func NewStaticPriceCatalogue(entries map[Key]string) *StaticPriceCatalogue {
	cat := &StaticPriceCatalogue{m: make(map[Key]string, len(entries))}
	for k, v := range entries {
		cat.m[k.normalised()] = v
	}
	return cat
}

// Resolve returns the Stripe Price ID + ok=true when the tuple matches,
// or "" + ok=false when not. Currency is lowercased before lookup so
// "USD" and "usd" hit the same entry.
func (c *StaticPriceCatalogue) Resolve(addonCode, tierCode, currency string) (string, bool) {
	v, ok := c.m[Key{AddonCode: addonCode, TierCode: tierCode, Currency: currency}.normalised()]
	return v, ok
}

// PlaceholderPriceCatalogue returns synthetic `price_TODO_<addon>_<tier>_<currency>`
// values for every tuple. Used as the safe-but-noisy fallback when the
// STRIPE_PRICE_CATALOGUE_SECRET_ID env var is unset (unit tests, fresh
// dev box). Always returns ok=true — downstream Stripe-call sites use
// IsPlaceholder to refuse before hitting the wire.
type PlaceholderPriceCatalogue struct{}

// NewPlaceholderPriceCatalogue returns a placeholder catalogue.
func NewPlaceholderPriceCatalogue() *PlaceholderPriceCatalogue {
	return &PlaceholderPriceCatalogue{}
}

// Resolve always returns ok=true and a synthetic placeholder ID.
func (c *PlaceholderPriceCatalogue) Resolve(addonCode, tierCode, currency string) (string, bool) {
	return fmt.Sprintf("price_TODO_%s_%s_%s", addonCode, tierCode, strings.ToLower(currency)), true
}

// IsPlaceholder reports whether the given Price ID is a CHO-1760
// placeholder (used by downstream Stripe-call sites to fail-loud
// before hitting the Stripe API with a synthetic ID).
func IsPlaceholder(stripePriceID string) bool {
	return strings.HasPrefix(stripePriceID, "price_TODO_")
}
