// Package backend dials the gRPC backends so that calls spread across all of their replicas.
package backend

import (
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ServiceConfig balances calls over every address the resolver returns, instead of the default
// pick_first, which sends every call of a channel to one replica.
const ServiceConfig = `{"loadBalancingConfig":[{"round_robin":{}}]}`

// Dial connects to address (host:port). The name is resolved through DNS and looked up again
// every refresh, so replicas that start or stop are picked up without waiting for a connection
// to fail. In Kubernetes the backend needs a headless Service, so DNS lists every ready pod; a
// ClusterIP Service resolves to one virtual IP and every call keeps going to one pod.
//
// Connections are lazy: a missing backend yields 503 on its routes only.
func Dial(address string, refresh time.Duration, extra ...grpc.DialOption) (*grpc.ClientConn, error) {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(newRefreshingBuilder(refresh)),
		// Skip the service config lookup in DNS: a resolver that is slow to answer missing
		// _grpc_config TXT records otherwise holds every call for ~10 s after each (re)connect.
		// The default service config below still applies.
		grpc.WithDisableServiceConfig(),
		grpc.WithDefaultServiceConfig(ServiceConfig),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	}
	return grpc.NewClient(refreshScheme+":///"+address, append(options, extra...)...)
}
