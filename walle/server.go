package walle

import (
	"context"
	"errors"

	walle_pb "github.com/zviadm/walle/proto/walle"
)

type Server struct {
}

func (s *Server) NewWriter(
	ctx context.Context, req *walle_pb.NewWriterRequest) (*walle_pb.BaseResponse, error) {
	return nil, errors.New("not implemented")
}
