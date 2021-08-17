package main

import (
	"fmt"

	"golang.org/x/net/context"

	pbrc "github.com/brotherlogic/recordcollection/proto"
	pb "github.com/brotherlogic/recordfanout/proto"
)

func (s *Server) Fanout(ctx context.Context, request *pb.FanoutRequest) (*pb.FanoutResponse, error) {
	if request.GetInstanceId() == 0 {
		s.Log("Unable to fanout empty request")
		return &pb.FanoutResponse{}, nil
	}
	s.Log(fmt.Sprintf("Running fanout for %v", request.GetInstanceId()))
	for _, server := range s.preCommit {
		conn, err := s.FDialServer(ctx, server)
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		client := pbrc.NewClientUpdateServiceClient(conn)
		_, err = client.ClientUpdate(ctx, &pbrc.ClientUpdateRequest{InstanceId: request.InstanceId})
		if err != nil {
			return nil, err
		}
	}

	conn, err := s.FDialServer(ctx, "recordcollection")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rcclient := pbrc.NewRecordCollectionServiceClient(conn)
	_, err = rcclient.CommitRecord(ctx, &pbrc.CommitRecordRequest{InstanceId: request.InstanceId})
	if err != nil {
		return nil, err
	}

	for _, server := range s.postCommit {
		conn, err := s.FDialServer(ctx, server)
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		client := pbrc.NewClientUpdateServiceClient(conn)
		_, err = client.ClientUpdate(ctx, &pbrc.ClientUpdateRequest{InstanceId: request.InstanceId})
		if err != nil {
			return nil, err
		}
	}

	return &pb.FanoutResponse{}, nil
}
