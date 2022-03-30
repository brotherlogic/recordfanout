package main

import (
	"fmt"
	"time"

	"golang.org/x/net/context"

	"github.com/brotherlogic/goserver/utils"
	pbrc "github.com/brotherlogic/recordcollection/proto"
	pb "github.com/brotherlogic/recordfanout/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	preLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "record_fanout_pre",
		Help:    "The latency of client requests",
		Buckets: []float64{.005 * 1000, .01 * 1000, .025 * 1000, .05 * 1000, .1 * 1000, .25 * 1000, .5 * 1000, 1 * 1000, 2.5 * 1000, 5 * 1000, 10 * 1000, 100 * 1000, 1000 * 1000},
	}, []string{"method"})
	commitLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "record_fanout_commit",
		Help:    "The latency of client requests",
		Buckets: []float64{.005 * 1000, .01 * 1000, .025 * 1000, .05 * 1000, .1 * 1000, .25 * 1000, .5 * 1000, 1 * 1000, 2.5 * 1000, 5 * 1000, 10 * 1000, 100 * 1000, 1000 * 1000},
	})
	postLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "record_fanout_post",
		Help:    "The latency of client requests",
		Buckets: []float64{.005 * 1000, .01 * 1000, .025 * 1000, .05 * 1000, .1 * 1000, .25 * 1000, .5 * 1000, 1 * 1000, 2.5 * 1000, 5 * 1000, 10 * 1000, 100 * 1000, 1000 * 1000},
	}, []string{"method"})
)

func (s *Server) Fanout(ctx context.Context, request *pb.FanoutRequest) (*pb.FanoutResponse, error) {
	ot := time.Now()
	if request.GetInstanceId() <= 0 {
		s.Log("Unable to fanout empty request")
		return &pb.FanoutResponse{}, nil
	}
	s.Log(fmt.Sprintf("Running fanout for %v", request.GetInstanceId()))
	serverTime := make(map[string]time.Duration)

	for _, server := range s.preCommit {
		t := time.Now()
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
		preLatency.With(prometheus.Labels{"method": server}).Observe(float64(time.Since(t).Milliseconds()))
		serverTime[server] = time.Since(t)
	}

	t := time.Now()
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
	serverTime["commit"] = time.Since(t)
	commitLatency.Observe(float64(time.Since(t).Milliseconds()))

	for _, server := range s.postCommit {
		t := time.Now()
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
		postLatency.With(prometheus.Labels{"method": server}).Observe(float64(time.Since(t).Milliseconds()))
		serverTime[server] = time.Since(t)
	}

	if time.Since(ot).Minutes() > 1 {
		key, _ := utils.GetContextKey(ctx)
		times := ""
		for key, value := range serverTime {
			if value > time.Second {
				times += fmt.Sprintf("%v took %v\n", key, value)
			}
		}
		s.RaiseIssue("Slow fanout", fmt.Sprintf("Fanout for %v took %v (%v\n%v)", request.GetInstanceId(), time.Since(ot), key, times))
	}

	return &pb.FanoutResponse{}, nil
}
