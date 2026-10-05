// grpc_legacy_alias.go: PaymentService answers the egg checkout under BOTH its
// renamed and its pre-rename method name, for the length of the W4-to-W5
// overlap.
//
// # Why
//
// ADR-254 D9 renamed the RPC CreateFamiliarEggCheckoutSession to
// CreateCompanionEggCheckoutSession. The two callers roll in DIFFERENT windows:
//
//	chora-gateway  W4, built on the renamed contracts. It is held on the old
//	               name by a client-side shim (its payments_legacy_egg_method.go)
//	               precisely because this alias did not exist yet.
//	chora-tenancy  W5, still compiled against the OLD name.
//
// The gateway shim covers one side only. The moment payments itself rolls
// renamed, tenancy becomes the caller left behind and the outage simply MOVES
// rather than closing. This file is the other side, and it is a PRECONDITION of
// the W5 payments roll rather than a task that follows it: with the alias in the
// image, the roll cannot break either caller by construction. A precondition
// that depends on somebody remembering at a flip hour is not a precondition.
//
// # Why not the shape chora-consumption used
//
// Consumption's alias registers one implementation under a second SERVICE name
// (FamiliarGrowth beside CompanionGrowth), which works because the whole service
// was renamed. Here only the METHOD moved: the service is still
// chora.services.payments.v1.PaymentService, and a gRPC server refuses to
// register the same service name twice. So the descriptor is COPIED with one
// extra MethodDesc appended, and registered once.
//
// That copy is the risk this file carries, and it is guarded by tests rather
// than by care: one asserts no generated method is dropped by the copy (a
// dropped RPC would surface as Unimplemented in production, not at compile
// time), and one asserts the generated package-level descriptor is never
// mutated in place, which would corrupt every other registration in the process.
//
// # Retire
//
// One roll AFTER the gateway and tenancy are both on the renamed method: delete
// this file, its call in main.go, the gateway's client-side shim, and the
// legacy method path from the payments Istio allowlist.
package main

import (
	"context"

	"google.golang.org/grpc"

	pb "github.com/apollo-chora/chora-contracts/gen/go/chora/services/payments/v1"
)

// legacyEggCheckoutMethodName is the pre-rename RPC name; legacyEggCheckout
// FullMethod is the path a caller dials and the Istio allowlist admits.
const (
	legacyEggCheckoutMethodName = "CreateFamiliarEggCheckoutSession"
	legacyEggCheckoutFullMethod = "/" + "chora.services.payments.v1.PaymentService" + "/" + legacyEggCheckoutMethodName
)

// legacyEggCheckoutHandler mirrors the generated handler for the renamed RPC,
// differing only in the FullMethod it reports to an interceptor, so a call
// arriving on the old path is attributed to the old path in logs and metrics
// rather than being silently relabelled as the new one.
//
// The request type is the RENAMED message. That is safe because the rename kept
// every field NUMBER, so the bytes an old caller sends decode identically; and
// it is preferable to keeping a second message type alive, which would be a
// second definition free to drift.
func legacyEggCheckoutHandler(
	srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor,
) (any, error) {
	in := new(pb.CreateCompanionEggCheckoutSessionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(pb.PaymentServiceServer).CreateCompanionEggCheckoutSession(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: legacyEggCheckoutFullMethod}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(pb.PaymentServiceServer).CreateCompanionEggCheckoutSession(
			ctx, req.(*pb.CreateCompanionEggCheckoutSessionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// paymentServiceDescWithLegacyEggAlias returns a COPY of the generated
// descriptor with the legacy egg method appended. It never mutates the
// generated one: that is a package-level var shared by every client and server
// in the process, and appending to its Methods slice in place would be a
// process-wide corruption that no test in this package would see.
func paymentServiceDescWithLegacyEggAlias() grpc.ServiceDesc {
	generated := pb.PaymentService_ServiceDesc

	methods := make([]grpc.MethodDesc, len(generated.Methods), len(generated.Methods)+1)
	copy(methods, generated.Methods)
	methods = append(methods, grpc.MethodDesc{
		MethodName: legacyEggCheckoutMethodName,
		Handler:    legacyEggCheckoutHandler,
	})

	desc := generated
	desc.Methods = methods
	return desc
}

// registerPaymentServiceWithLegacyEggAlias registers PaymentService once, under
// the descriptor that answers to both egg-checkout names. Use INSTEAD of
// pb.RegisterPaymentServiceServer; calling both would panic on a duplicate
// service registration.
func registerPaymentServiceWithLegacyEggAlias(reg grpc.ServiceRegistrar, srv pb.PaymentServiceServer) {
	desc := paymentServiceDescWithLegacyEggAlias()
	reg.RegisterService(&desc, srv)
}
