// grpc_legacy_alias_test.go: the egg-checkout method alias, proven over a real
// gRPC server rather than by inspecting a descriptor.
//
// ADR-254 D9 renamed the RPC CreateFamiliarEggCheckoutSession to
// CreateCompanionEggCheckoutSession. Callers roll in different windows, so for
// one window the server must answer to BOTH names or somebody's checkout dies:
//
//	chora-gateway  rolls at W4, built on the renamed contracts (new name only,
//	               currently held on the old name by a client-side shim)
//	chora-tenancy  rolls at W5, still compiled against the OLD name
//
// The gateway's shim covers ONE side. When payments itself rolls renamed,
// tenancy is the caller left behind, and the outage simply moves. This alias is
// the other side, and the W5 roll is a PRECONDITION on it rather than a task
// that follows it: a precondition that depends on someone remembering is not a
// precondition.
//
// The tests dial a real server over bufconn and invoke the LEGACY method string
// by hand, because a descriptor assertion would only prove the table was built,
// not that gRPC routes on it.
package main

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/apollo-chora/chora-contracts/gen/go/chora/services/payments/v1"
)

// aliasProbeServer records which RPC ran. It embeds the generated Unimplemented
// so it satisfies PaymentServiceServer without hand-writing the other RPCs.
type aliasProbeServer struct {
	pb.UnimplementedPaymentServiceServer
	eggCalls  int
	gotTenant string
}

func (s *aliasProbeServer) CreateCompanionEggCheckoutSession(
	_ context.Context, in *pb.CreateCompanionEggCheckoutSessionRequest,
) (*pb.CreateCompanionEggCheckoutSessionResponse, error) {
	s.eggCalls++
	s.gotTenant = in.GetTenantId()
	return &pb.CreateCompanionEggCheckoutSessionResponse{PurchaseId: "purchase-1"}, nil
}

// dialAliasProbe starts a server with the alias registration and returns a
// raw connection, so a test can invoke ANY method string it likes.
func dialAliasProbe(t *testing.T) (*grpc.ClientConn, *aliasProbeServer) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	probe := &aliasProbeServer{}
	registerPaymentServiceWithLegacyEggAlias(srv, probe)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, probe
}

func invokeEgg(t *testing.T, conn *grpc.ClientConn, method string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	in := &pb.CreateCompanionEggCheckoutSessionRequest{TenantId: "11111111-1111-7111-8111-111111111111"}
	out := new(pb.CreateCompanionEggCheckoutSessionResponse)
	return conn.Invoke(ctx, method, in, out)
}

// THE overlap guarantee: a caller still compiled against the OLD name is served.
func TestLegacyEggAlias_OldMethodNameIsServed(t *testing.T) {
	conn, probe := dialAliasProbe(t)

	if err := invokeEgg(t, conn, legacyEggCheckoutFullMethod); err != nil {
		t.Fatalf("legacy method %s was not served: %v (chora-tenancy still dials this until its W5 roll)",
			legacyEggCheckoutFullMethod, err)
	}
	if probe.eggCalls != 1 {
		t.Errorf("handler ran %d times via the legacy name; want 1", probe.eggCalls)
	}
	if probe.gotTenant != "11111111-1111-7111-8111-111111111111" {
		t.Errorf("the request did not reach the handler intact: tenant=%q", probe.gotTenant)
	}
}

// The new name must keep working: the alias ADDS a route, it does not move one.
func TestLegacyEggAlias_NewMethodNameStillServed(t *testing.T) {
	conn, probe := dialAliasProbe(t)

	if err := invokeEgg(t, conn, pb.PaymentService_CreateCompanionEggCheckoutSession_FullMethodName); err != nil {
		t.Fatalf("the renamed method was not served: %v", err)
	}
	if probe.eggCalls != 1 {
		t.Errorf("handler ran %d times via the new name; want 1", probe.eggCalls)
	}
}

// ONE implementation behind both names, so the two callers cannot drift into
// different behaviour during the overlap.
func TestLegacyEggAlias_BothNamesReachTheSameHandler(t *testing.T) {
	conn, probe := dialAliasProbe(t)

	if err := invokeEgg(t, conn, legacyEggCheckoutFullMethod); err != nil {
		t.Fatalf("legacy invoke: %v", err)
	}
	if err := invokeEgg(t, conn, pb.PaymentService_CreateCompanionEggCheckoutSession_FullMethodName); err != nil {
		t.Fatalf("new invoke: %v", err)
	}
	if probe.eggCalls != 2 {
		t.Errorf("handler ran %d times across both names; want 2 (one implementation, two routes)", probe.eggCalls)
	}
}

// The rest of PaymentService must still be registered. Building a ServiceDesc
// by hand risks dropping methods, and a dropped RPC would surface as
// Unimplemented in production rather than at compile time.
func TestLegacyEggAlias_EveryGeneratedMethodSurvives(t *testing.T) {
	desc := paymentServiceDescWithLegacyEggAlias()

	generated := make(map[string]bool, len(pb.PaymentService_ServiceDesc.Methods))
	for _, m := range pb.PaymentService_ServiceDesc.Methods {
		generated[m.MethodName] = true
	}
	aliased := make(map[string]bool, len(desc.Methods))
	for _, m := range desc.Methods {
		aliased[m.MethodName] = true
	}
	for name := range generated {
		if !aliased[name] {
			t.Errorf("method %q was DROPPED by the alias descriptor; it would be Unimplemented in production", name)
		}
	}
	if len(desc.Methods) != len(pb.PaymentService_ServiceDesc.Methods)+1 {
		t.Errorf("alias descriptor has %d methods; want the generated %d plus exactly one alias",
			len(desc.Methods), len(pb.PaymentService_ServiceDesc.Methods))
	}
	if desc.ServiceName != pb.PaymentService_ServiceDesc.ServiceName {
		t.Errorf("service name changed to %q; only the METHOD was renamed, so the service name must not move",
			desc.ServiceName)
	}
}

// The generated descriptor must not be mutated: it is a package-level var, and
// editing it in place would corrupt every other registration in the process.
func TestLegacyEggAlias_DoesNotMutateTheGeneratedDescriptor(t *testing.T) {
	before := len(pb.PaymentService_ServiceDesc.Methods)
	_ = paymentServiceDescWithLegacyEggAlias()
	_ = paymentServiceDescWithLegacyEggAlias()
	if got := len(pb.PaymentService_ServiceDesc.Methods); got != before {
		t.Fatalf("the generated ServiceDesc grew from %d to %d methods; the alias must copy, never append in place",
			before, got)
	}
}

// The comment on legacyEggCheckoutHandler claims a call on the old path is
// ATTRIBUTED to the old path rather than silently relabelled as the new one.
// That is a behavioural claim, so it is pinned rather than asserted in prose:
// an interceptor (the shape used for access logging and metrics) must see the
// LEGACY FullMethod, or every overlap call would be counted as new-name traffic
// and nobody could tell when the old path had actually gone quiet, which is the
// signal that says the alias is safe to retire.
func TestLegacyEggAlias_InterceptorSeesTheLegacyMethodName(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	var seen []string
	srv := grpc.NewServer(grpc.UnaryInterceptor(
		func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			seen = append(seen, info.FullMethod)
			return h(ctx, req)
		}))
	registerPaymentServiceWithLegacyEggAlias(srv, &aliasProbeServer{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if err := invokeEgg(t, conn, legacyEggCheckoutFullMethod); err != nil {
		t.Fatalf("legacy invoke through an interceptor: %v", err)
	}
	if err := invokeEgg(t, conn, pb.PaymentService_CreateCompanionEggCheckoutSession_FullMethodName); err != nil {
		t.Fatalf("new invoke through an interceptor: %v", err)
	}

	if len(seen) != 2 {
		t.Fatalf("interceptor saw %d calls; want 2", len(seen))
	}
	if seen[0] != legacyEggCheckoutFullMethod {
		t.Errorf("the legacy call was reported as %q; want %q, or overlap traffic is indistinguishable from new-name traffic",
			seen[0], legacyEggCheckoutFullMethod)
	}
	if seen[1] != pb.PaymentService_CreateCompanionEggCheckoutSession_FullMethodName {
		t.Errorf("the new call was reported as %q; want the generated name", seen[1])
	}
}

// A body that will not decode must fail, not reach the handler. Guards the
// hand-written decode arm the copied descriptor introduced.
func TestLegacyEggAlias_UndecodableBodyNeverReachesTheHandler(t *testing.T) {
	probe := &aliasProbeServer{}
	_, err := legacyEggCheckoutHandler(probe, context.Background(),
		func(any) error { return errDecode }, nil)
	if err == nil {
		t.Fatal("a decode failure was swallowed")
	}
	if probe.eggCalls != 0 {
		t.Errorf("handler ran %d times on an undecodable body; want 0", probe.eggCalls)
	}
}

var errDecode = errors.New("malformed body")
