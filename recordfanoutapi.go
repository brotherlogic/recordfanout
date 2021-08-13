package main

import (
	"fmt"

	"golang.org/x/net/context"

	pbrc "github.com/brotherlogic/recordcollection/proto"
	pb "github.com/brotherlogic/recordfanout/proto"
)

func (s *Server) Fanout(ctx context.Context, request *pb.FanoutRequest) (*pb.FanoutResponse, error) {
	if request.GetInstanceId() == 0 {
		s.Log(fmt.Sprintf("Unable to fanout empty request"))
		return &pb.FanoutResponse{}, nil
	}
	for _, server := range s.preCommit {
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

	conn, err := s.FDialServer(ctx, "recordcollection")

	if err != nil {
		return nil, err
	}

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

		client := pbrc.NewClientUpdateServiceClient(conn)
		_, err = client.ClientUpdate(ctx, &pbrc.ClientUpdateRequest{InstanceId: request.InstanceId})
		if err != nil {
			return nil, err
		}

		conn.Close()
	}

	return &pb.FanoutResponse{}, nil
}
