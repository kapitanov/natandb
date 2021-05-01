package proto

import (
	"context"
	"net"

	"github.com/kapitanov/natandb/pkg/db"
	"github.com/kapitanov/natandb/pkg/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var serverLog = log.New("server")

// Server is a GRRC service wrapper
type Server interface {
	// Start starts serer
	Start() error
	// Close shuts server down
	Close() error
}

type serverImpl struct {
	server   *grpc.Server
	engine   db.Engine
	endpoint string
	listener net.Listener
}

// NewServer creates new server instance
func NewServer(engine db.Engine, endpoint string) Server {
	server := grpc.NewServer()

	return &serverImpl{
		server:   server,
		engine:   engine,
		endpoint: endpoint,
		listener: nil,
	}
}

// Start starts serer
func (s *serverImpl) Start() error {
	serverLog.Verbosef("starting server")
	RegisterServiceServer(s.server, s)

	listener, err := net.Listen("tcp", s.endpoint)
	if err != nil {
		return err
	}

	go func() {
		_ = s.server.Serve(listener)
	}()

	serverLog.Printf("server is running at \"tcp://%s\"", s.endpoint)
	s.listener = listener
	return nil
}

// Close shuts server down
func (s *serverImpl) Close() error {
	serverLog.Verbosef("shutting down")
	s.server.GracefulStop()
	serverLog.Verbosef("shutdown completed")
	return nil
}

// List returns paged list of DB keys (with values)
// Optionally list might be filtered by key prefix
func (s *serverImpl) List(context context.Context, request *ListRequest) (*PagedNodeList, error) {
	list, err := s.engine.List(db.Key(request.Prefix), uint(request.Skip), uint(request.Limit), request.Version)
	if err != nil {
		return nil, mapServerError(err)
	}

	response := PagedNodeList{
		Nodes:      make([]*Node, len(list.Nodes)),
		TotalCount: uint32(list.TotalCount),
		Version:    list.Version,
	}

	for i := range list.Nodes {
		response.Nodes[i] = serverMapNode(list.Nodes[i])
	}
	return &response, nil
}

// GetVersion returns current data version
func (s *serverImpl) GetVersion(context context.Context, request *None) (*DBVersion, error) {
	version := s.engine.GetVersion()
	response := DBVersion{
		Version: version,
	}
	return &response, nil
}

// GetValue gets a node value by its key
// If specified node doesn't exist, a ErrNoSuchKey error is returned
func (s *serverImpl) GetValue(context context.Context, request *GetRequest) (*Node, error) {
	node, err := s.engine.Get(db.Key(request.Key))
	if err != nil {
		return nil, mapServerError(err)
	}

	response := serverMapNode(node)
	return response, nil
}

// SetValue sets a node value, rewritting its value if node already exists
// If specified node doesn't exists, it will be created
func (s *serverImpl) SetValue(context context.Context, request *SetValueRequest) (*Node, error) {
	values := make([]db.Value, len(request.Values))
	for i := range request.Values {
		values[i] = db.Value(request.Values[i])
	}

	node, err := s.engine.Set(db.Key(request.Key), values)
	if err != nil {
		return nil, mapServerError(err)
	}

	response := serverMapNode(node)
	return response, nil
}

// AddValue defines an "append value" operation
// If specified node doesn't exists, it will be created
// A specified value will be added to node even if it already exists
func (s *serverImpl) AddValue(context context.Context, request *AddValueRequest) (*Node, error) {
	node, err := s.engine.AddValue(db.Key(request.Key), db.Value(request.Value))
	if err != nil {
		return nil, mapServerError(err)
	}

	response := serverMapNode(node)
	return response, nil
}

// AddUniqueValue defines an "append value" operation
// If specified node doesn't exists, it will be created
// If node already contains the same value and "unique" parameter is set to "true", a ErrDuplicateValue error is returned
func (s *serverImpl) AddUniqueValue(context context.Context, request *AddUniqueValueRequest) (*Node, error) {
	node, err := s.engine.AddUniqueValue(db.Key(request.Key), db.Value(request.Value))
	if err != nil {
		return nil, mapServerError(err)
	}

	response := serverMapNode(node)
	return response, nil
}

// RemoveValue defines an "remove value" operation
// If specified node doesn't exist, a ErrNoSuchKey error is returned
// If specified value doesn't exist within a node, a ErrNoSuchValue error is returned
func (s *serverImpl) RemoveValue(context context.Context, request *RemoveValueRequest) (*Node, error) {
	node, err := s.engine.RemoveValue(db.Key(request.Key), db.Value(request.Value))
	if err != nil {
		return nil, mapServerError(err)
	}

	response := serverMapNode(node)
	return response, nil
}

// RemoveAllValues defines an "remove value" operation
// If specified node doesn't exist, a ErrNoSuchKey error is returned
// If node contains specified value multiple times, all values are removed
// If specified value doesn't exist within a node, a ErrNoSuchValue error is returned
func (s *serverImpl) RemoveAllValues(context context.Context, request *RemoveAllValuesRequest) (*Node, error) {
	node, err := s.engine.RemoveAllValues(db.Key(request.Key), db.Value(request.Value))
	if err != nil {
		return nil, mapServerError(err)
	}

	response := serverMapNode(node)
	return response, nil
}

// RemoveKey removes a key completely
// If specified node doesn't exist, a ErrNoSuchKey error is returned
func (s *serverImpl) RemoveKey(context context.Context, request *RemoveKeyRequest) (*None, error) {
	err := s.engine.RemoveKey(db.Key(request.Key))
	if err != nil {
		return nil, mapServerError(err)
	}

	response := &None{}
	return response, nil
}

func serverMapNode(node *db.Node) *Node {
	values := make([][]byte, len(node.Values))
	for i := range node.Values {
		values[i] = []byte(node.Values[i])
	}

	return &Node{
		Key:     string(node.Key),
		Values:  values,
		Version: node.Version,
	}
}

func mapServerError(err error) error {
	switch err {
	case context.Canceled:
		return status.Error(codes.Canceled, err.Error())
	case context.DeadlineExceeded:
		return status.Error(codes.DeadlineExceeded, err.Error())
	}

	switch e := err.(type) {
	case db.Error:
		switch e {
		case db.ErrNoSuchKey:
			return status.Error(codes.NotFound, e.String())
		case db.ErrDuplicateValue:
			return status.Error(codes.AlreadyExists, e.String())
		case db.ErrDataOutOfDate:
			return status.Error(codes.FailedPrecondition, e.String())
		case db.ErrNoSuchValue:
			return status.Error(codes.InvalidArgument, e.String())
		}
	}

	panic(err)
	return err
}
