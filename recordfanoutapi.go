package main

import (
	"golang.org/x/net/context"

	pbrc "github.com/brotherlogic/recordcollection/proto"
	pb "github.com/brotherlogic/recordfanout/proto"
)

func (s *Server) Fanout(ctx context.Context, request *pb.FanoutRequest) (*pb.FanoutResponse, error) {
	for _, server := range s.postCommit {
		conn, err := s.FDialServer(ctx, server)

		if err != nil {
			return nil, err
		}

		client := pbrc.NewClientUpdateServiceClient(conn)
		_, err = client.ClientUpdate(ctx, &pbrc.ClientUpdateRequest{InstanceId: request.InstanceId})
		if err != nil {
			return nil, err
		}

		conn.Close()
	}

	return &pb.FanoutResponse{}, nil
}
