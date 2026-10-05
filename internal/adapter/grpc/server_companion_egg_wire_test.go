// server_companion_egg_wire_test.go — ADR-254 D9 wire-name guard.
//
// The generated PaymentServiceServer interface names the egg checkout
// CreateCompanionEggCheckoutSession. Server embeds
// pb.UnimplementedPaymentServiceServer, so a Server that defines only the
// PRE-RENAME Go method still satisfies the interface at compile time and
// answers the renamed RPC with codes.Unimplemented at runtime. Nothing in the
// build or in the existing suite can see that: both wire names route through
// the interface method, so BOTH go dark together.
//
// This test calls the RPC through the generated interface, which is the only
// accessor that reaches the embedded fallback.
package grpc_test

import (
	"context"
	"testing"

	pb "github.com/apollo-chora/chora-contracts/gen/go/chora/services/payments/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCompanionEggCheckout_ServedThroughGeneratedInterface(t *testing.T) {
	t.Parallel()
	var srv pb.PaymentServiceServer = newServer(t)
	resp, err := srv.CreateCompanionEggCheckoutSession(context.Background(), &pb.CreateCompanionEggCheckoutSessionRequest{
		IdempotencyKey: "k-wire",
		TenantId:       tTenantID,
		LearnerGcid:    tLearnerGCID,
		EggSku:         "egg.standard.v1",
		AmountCents:    1900,
		Currency:       "SGD",
	})
	if st, ok := status.FromError(err); ok && st.Code() == codes.Unimplemented {
		t.Fatalf("renamed RPC fell through to UnimplementedPaymentServiceServer: %v", err)
	}
	if err != nil {
		t.Fatalf("CreateCompanionEggCheckoutSession: %v", err)
	}
	if resp.GetPurchaseId() == "" {
		t.Fatal("purchase_id empty")
	}
}
