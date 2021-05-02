package proto

import (
	"context"

	l "github.com/kapitanov/natandb/pkg/log"
	"google.golang.org/grpc"
)

var clientLog = l.New("client")

// Client is a client for NatanDB service
type Client interface {
	ServiceClient
	// Close shuts down client connection
	Close() error
}

type clientImpl struct {
	connection *grpc.ClientConn
	client     ServiceClient
}

// NewClient creates new client and connects it to specified remote service
func NewClient(address string) (Client, error) {
	clientLog.Printf("connecting to %s...", address)
	connection, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		clientLog.Printf("unable to connect. %s", err)
		return nil, err
	}

	clientLog.Printf("connected to %s", address)
	client := NewServiceClient(connection)
	c := clientImpl{
		connection: connection,
		client:     client,
	}
	return &c, nil
}

// List returns paged list of DB keys (with values)
// Optionally list might be filtered by key prefix
func (c *clientImpl) List(ctx context.Context, in *ListRequest, opts ...grpc.CallOption) (*PagedNodeList, error) {
	return c.client.List(ctx, in, opts...)
}

// GetVersion returns current data version
func (c *clientImpl) GetVersion(ctx context.Context, in *None, opts ...grpc.CallOption) (*DBVersion, error) {
	return c.client.GetVersion(ctx, in, opts...)
}

// GetValue gets a node value by its key
// If specified node doesn't exist, a ErrNoSuchKey error is returned
func (c *clientImpl) GetValue(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*Node, error) {
	return c.client.GetValue(ctx, in, opts...)
}

// SetValue sets a node value, rewriting its value if node already exists
// If specified node doesn't exists, it will be created
func (c *clientImpl) SetValue(ctx context.Context, in *SetValueRequest, opts ...grpc.CallOption) (*Node, error) {
	return c.client.SetValue(ctx, in, opts...)
}

// AddValue defines an "append value" operation
// If specified node doesn't exists, it will be created
// A specified value will be added to node even if it already exists
func (c *clientImpl) AddValue(ctx context.Context, in *AddValueRequest, opts ...grpc.CallOption) (*Node, error) {
	return c.client.AddValue(ctx, in, opts...)
}

// AddUniqueValue defines an "append value" operation
// If specified node doesn't exists, it will be created
// If node already contains the same value and "unique" parameter is set to "true", a ErrDuplicateValue error is returned
func (c *clientImpl) AddUniqueValue(ctx context.Context, in *AddUniqueValueRequest, opts ...grpc.CallOption) (*Node, error) {
	return c.client.AddUniqueValue(ctx, in, opts...)
}

// RemoveValue defines an "remove value" operation
// If specified node doesn't exist, a ErrNoSuchKey error is returned
// If specified value doesn't exist within a node, a ErrNoSuchValue error is returned
func (c *clientImpl) RemoveValue(ctx context.Context, in *RemoveValueRequest, opts ...grpc.CallOption) (*Node, error) {
	return c.client.RemoveValue(ctx, in, opts...)
}

// RemoveAllValues defines an "remove value" operation
// If specified node doesn't exist, a ErrNoSuchKey error is returned
// If node contains specified value multiple times, all values are removed
// If specified value doesn't exist within a node, a ErrNoSuchValue error is returned
func (c *clientImpl) RemoveAllValues(ctx context.Context, in *RemoveAllValuesRequest, opts ...grpc.CallOption) (*Node, error) {
	return c.client.RemoveAllValues(ctx, in, opts...)
}

// RemoveKey removes a key completely
// If specified node doesn't exist, a ErrNoSuchKey error is returned
func (c *clientImpl) RemoveKey(ctx context.Context, in *RemoveKeyRequest, opts ...grpc.CallOption) (*None, error) {
	return c.client.RemoveKey(ctx, in, opts...)
}

// Close shuts down client connection
func (c *clientImpl) Close() error {
	clientLog.Printf("disconnecting from %s", c.connection.Target())
	err := c.connection.Close()
	if err != nil {
		return err
	}

	return nil
}
