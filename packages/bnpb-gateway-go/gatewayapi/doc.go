// Package gatewayapi holds the chi strict-server interfaces, models and embedded spec that
// oapi-codegen generates from contracts/openapi/gateway.yaml (gateway.gen.go, not committed).
//
//	npx nx run bnpb-gateway-go:generate-openapi
package gatewayapi

//go:generate go tool oapi-codegen -config ../oapi-codegen.yaml ../../../contracts/openapi/gateway.yaml
