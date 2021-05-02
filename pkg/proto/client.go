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

// Version returns current data version
func (c *clientImpl) Version(ctx context.Context, in *None, opts ...grpc.CallOption) (*DBVersion, error) {
	return c.client.Version(ctx, in, opts...)
}

// Get gets a node value by its key
// If specified node doesn't exist, a ErrNoSuchKey error is returned
func (c *clientImpl) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*Node, error) {
	return c.client.Get(ctx, in, opts...)
}

// Set sets a node value, rewriting its value if node already exists
// If specified node doesn't exists, it will be created
func (c *clientImpl) Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*Node, error) {
	return c.client.Set(ctx, in, opts...)
}

// Add defines an "append value" operation
// If specified node doesn't exists, it will be created
// A specified value will be added to node even if it already exists
// If node already contains the same value and "unique" parameter is set to "true", a ErrDuplicateValue error is returned
func (c *clientImpl) Add(ctx context.Context, in *AddRequest, opts ...grpc.CallOption) (*Node, error) {
	return c.client.Add(ctx, in, opts...)
}

// Remove defines an "remove value" operation
// If specified node doesn't exist, a ErrNoSuchKey error is returned
// If specified value doesn't exist within a node, a ErrNoSuchValue error is returned
// If node contains specified value multiple times, all values are removed
// (unless a "all" parameter is set to "false"
func (c *clientImpl) Remove(ctx context.Context, in *RemoveRequest, opts ...grpc.CallOption) (*Node, error) {
	return c.client.Remove(ctx, in, opts...)
}

// Delete removes a key completely
// If specified node doesn't exist, a ErrNoSuchKey error is returned
func (c *clientImpl) Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*None, error) {
	return c.client.Delete(ctx, in, opts...)
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
