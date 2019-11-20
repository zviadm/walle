package main

import (
	"log"
	"net"

	"google.golang.org/grpc"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/walle"
)

func main() {
	ws := new(walle.Server)
	s := grpc.NewServer()
	walle_pb.RegisterWalleServer(s, ws)

	l, err := net.Listen("tcp", ":5005") // TODO(zviad): choose proper port.
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	if err := s.Serve(l); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
