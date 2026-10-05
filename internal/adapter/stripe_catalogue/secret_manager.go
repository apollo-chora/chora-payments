// secret_manager.go — JSON-blob-from-Secret-Manager loader for
// PriceCatalogue. CHO-1760.
package stripe_catalogue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// SecretFetcher is the minimal secret-resolver surface this package
// needs. libs/chora-go-common/secrets.Client + libs/chora-go-common/
// secrets.StubClient both satisfy it (no import here so this package
// stays test-isolated).
type SecretFetcher interface {
	GetSecret(ctx context.Context, name string) (string, error)
}

// NewEnvPriceCatalogue eager-loads + parses the JSON blob
// stored under `secretID` in the env-backed secret store, returning a fully-populated
// StaticPriceCatalogue. Failure at any step (fetch / parse / key
// validation) bubbles up so main.go can `log.Fatalf` at boot per
// fail-loud secrets policy.
//
// Expected JSON shape:
//
//	{
//	  "tms:starter:usd": "price_xxx",
//	  "tms:pro:usd":     "price_yyy",
//	  ...
//	}
//
// Each key MUST be `<addon_code>:<tier_code>:<currency>` (currency is
// lowercased to match the StaticPriceCatalogue normalisation).
func NewEnvPriceCatalogue(ctx context.Context, fetcher SecretFetcher, secretID string) (*StaticPriceCatalogue, error) {
	if fetcher == nil {
		return nil, errors.New("stripe_catalogue: fetcher required")
	}
	if secretID == "" {
		return nil, errors.New("stripe_catalogue: secretID required")
	}
	blob, err := fetcher.GetSecret(ctx, secretID)
	if err != nil {
		return nil, fmt.Errorf("stripe_catalogue: fetch %q: %w", secretID, err)
	}
	var raw map[string]string
	if err := json.Unmarshal([]byte(blob), &raw); err != nil {
		return nil, fmt.Errorf("stripe_catalogue: parse %q: %w", secretID, err)
	}
	entries := make(map[Key]string, len(raw))
	for k, v := range raw {
		parts := strings.Split(k, ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("stripe_catalogue: key %q must be addon:tier:currency", k)
		}
		entries[Key{
			AddonCode: parts[0],
			TierCode:  parts[1],
			Currency:  parts[2],
		}] = v
	}
	return NewStaticPriceCatalogue(entries), nil
}
