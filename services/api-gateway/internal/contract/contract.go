// Package contract exposes the embedded OpenAPI document the gateway was generated from.
package contract

import (
	"context"
	"fmt"
	"strconv"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-gateway-go/gatewayapi"
)

// Operation is one method+path of the contract.
type Operation struct {
	Method      string
	Path        string
	OperationID string
	statuses    map[int]bool
}

// Contract answers which statuses each operation may return, straight from gateway.yaml.
type Contract struct {
	Doc        *openapi3.T
	operations map[string]*Operation
}

// Load reads the spec oapi-codegen embedded into gatewayapi.
func Load() (*Contract, error) {
	doc, err := gatewayapi.GetSwagger()
	if err != nil {
		return nil, fmt.Errorf("load embedded OpenAPI: %w", err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		return nil, fmt.Errorf("embedded OpenAPI is invalid: %w", err)
	}
	c := &Contract{Doc: doc, operations: map[string]*Operation{}}
	for path, item := range doc.Paths.Map() {
		for method, op := range item.Operations() {
			statuses := map[int]bool{}
			for code := range op.Responses.Map() {
				if status, err := strconv.Atoi(code); err == nil {
					statuses[status] = true
				}
			}
			c.operations[key(method, path)] = &Operation{Method: method, Path: path, OperationID: op.OperationID, statuses: statuses}
		}
	}
	return c, nil
}

func key(method, path string) string { return method + " " + path }

// Operation looks up an operation by method and route pattern (chi patterns equal OpenAPI paths).
func (c *Contract) Operation(method, path string) *Operation { return c.operations[key(method, path)] }

func (c *Contract) Operations() []*Operation {
	ops := make([]*Operation, 0, len(c.operations))
	for _, op := range c.operations {
		ops = append(ops, op)
	}
	return ops
}

// Allows reports whether the operation documents this response status.
func (o *Operation) Allows(status int) bool { return o != nil && o.statuses[status] }
