package main

import (
	"golang.org/x/net/context"

	pb "github.com/brotherlogic/recordfanout/proto"
)

func (s *Server) Fanout(ctx context.Context, request *pb.FanoutRequest) (*pb.FanoutResponse, error) {
	return &pb.FanoutResponse{}, nil
}
