package walle

import (
	"context"
	"io"

	"google.golang.org/grpc"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
)

type client struct {
	Client
	serverId string
	self     *clientSelf
}

func wrapClient(c Client, serverId string, s *Server) *client {
	return &client{Client: c, serverId: serverId, self: &clientSelf{s: s}}
}

func (c *client) ForServer(serverId string) (walle_pb.WalleClient, error) {
	if serverId != c.serverId {
		return c.Client.ForServer(serverId)
	}
	return c.self, nil
}

// clientSelf exposes Server object with WalleClient interface. This is
// useful to make local calls with WalleClient api, but without going through
// any of the actual gRPC stack for performance.
type clientSelf struct{ s *Server }

func (c *clientSelf) PutEntryInternal(
	ctx context.Context,
	in *walle_pb.PutEntryInternalRequest,
	opts ...grpc.CallOption) (*walleapi.Empty, error) {
	return c.s.PutEntryInternal(ctx, in)
}

func (c *clientSelf) NewWriter(
	ctx context.Context,
	in *walle_pb.NewWriterRequest,
	opts ...grpc.CallOption) (*walleapi.Empty, error) {
	return c.s.NewWriter(ctx, in)
}

func (c *clientSelf) WriterInfo(
	ctx context.Context,
	in *walle_pb.WriterInfoRequest,
	opts ...grpc.CallOption) (*walle_pb.WriterInfoResponse, error) {
	return c.s.WriterInfo(ctx, in)
}

func (c *clientSelf) TailEntries(
	ctx context.Context,
	in *walle_pb.TailEntriesRequest,
	opts ...grpc.CallOption) (walle_pb.Walle_TailEntriesClient, error) {
	sClient, sServer, closeF := newEntriesStreams(ctx)
	go func() {
		err := c.s.TailEntries(in, sServer)
		closeF(err)
	}()
	return sClient, nil
}

func (c *clientSelf) ReadEntries(
	ctx context.Context,
	in *walle_pb.ReadEntriesRequest,
	opts ...grpc.CallOption) (walle_pb.Walle_ReadEntriesClient, error) {
	sClient, sServer, closeF := newEntriesStreams(ctx)
	go func() {
		err := c.s.ReadEntries(in, sServer)
		closeF(err)
	}()
	return sClient, nil
}

func newEntriesStreams(ctx context.Context) (
	*entriesStreamClient, *entriesStreamServer, func(err error)) {
	s := &entriesStream{ctx: ctx, buffer: make(chan entriesStreamEntry)}
	return &entriesStreamClient{entriesStream: s},
		&entriesStreamServer{entriesStream: s},
		s.close
}

type entriesStream struct {
	ctx    context.Context
	buffer chan entriesStreamEntry
}

type entriesStreamClient struct {
	*entriesStream
	grpc.ClientStream
}
type entriesStreamServer struct {
	*entriesStream
	grpc.ServerStream
}

type entriesStreamEntry struct {
	Entry *walleapi.Entry
	Err   error
}

func (s *entriesStreamClient) Context() context.Context {
	return s.ctx
}
func (s *entriesStreamServer) Context() context.Context {
	return s.ctx
}

func (s *entriesStream) Recv() (*walleapi.Entry, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case r := <-s.buffer:
		return r.Entry, r.Err
	}
}

func (s *entriesStream) Send(e *walleapi.Entry) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.buffer <- entriesStreamEntry{Entry: e}:
		return nil
	}
}

func (s *entriesStream) close(err error) {
	if err == nil {
		err = io.EOF
	}
	select {
	case <-s.ctx.Done():
	case s.buffer <- entriesStreamEntry{Err: err}:
	}
}
