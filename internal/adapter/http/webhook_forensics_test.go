package http

import "testing"

func TestBestEffortEventMeta(t *testing.T) {
	tests := []struct {
		name           string
		body           string
		wantID, wantTy string
	}{
		{
			name:   "valid stripe event",
			body:   `{"id":"evt_1NXa2b","type":"checkout.session.completed","data":{"object":{}}}`,
			wantID: "evt_1NXa2b", wantTy: "checkout.session.completed",
		},
		{
			name:   "id only",
			body:   `{"id":"evt_x"}`,
			wantID: "evt_x", wantTy: "",
		},
		{
			name:   "malformed json — best effort returns empties, never panics",
			body:   `{not json`,
			wantID: "", wantTy: "",
		},
		{
			name:   "empty body",
			body:   ``,
			wantID: "", wantTy: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, ty := bestEffortEventMeta([]byte(tc.body))
			if id != tc.wantID || ty != tc.wantTy {
				t.Errorf("bestEffortEventMeta = (%q, %q), want (%q, %q)", id, ty, tc.wantID, tc.wantTy)
			}
		})
	}
}
