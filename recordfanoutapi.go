package main

import (
	"fmt"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	errors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "record_fanout_errors",
		Help: "Errors by client",
	}, []string{"client", "code"})
)

func (s *Server) Fanout(ctx context.Context, request *pb.FanoutRequest) (*pb.FanoutResponse, error) {
	ot := time.Now()

	if request.GetInstanceId() <= 0 {
		s.CtxLog(ctx, fmt.Sprintf("Dropping key: %v", request))
		return &pb.FanoutResponse{}, nil
	}

	conn, err := s.FDialServer(ctx, "recordcollection")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := pbrc.NewRecordCollectionServiceClient(conn)
	rec, err := client.GetRecord(ctx, &pbrc.GetRecordRequest{InstanceId: request.GetInstanceId()})
	if err != nil {
		// Record has been deleted
		if status.Code(err) == codes.OutOfRange {
			return &pb.FanoutResponse{}, nil
		}
		return nil, err
	}
	s.CtxLog(ctx, fmt.Sprintf("Running on %v", rec))

	// Hard skip everything but unknown or 12 inch
	if rec.GetRecord().GetMetadata().GetFiledUnder() != pbrc.ReleaseMetadata_FILE_UNKNOWN &&
		rec.GetRecord().GetMetadata().GetFiledUnder() != pbrc.ReleaseMetadata_FILE_12_INCH &&
		rec.GetRecord().GetMetadata().GetFiledUnder() != pbrc.ReleaseMetadata_FILE_7_INCH &&
		rec.GetRecord().GetMetadata().GetFiledUnder() != pbrc.ReleaseMetadata_FILE_CD {
		return &pb.FanoutResponse{}, nil
	}

	defer func() {
		s.CtxLog(ctx, fmt.Sprintf("FanoutTook %v", time.Since(ot)))
	}()

	if request.GetInstanceId() <= 0 {
		s.CtxLog(ctx, "Unable to fanout empty request")
		return &pb.FanoutResponse{}, nil
	}
	s.CtxLog(ctx, fmt.Sprintf("Running fanout for %v", request.GetInstanceId()))
	serverTime := make(map[string]time.Duration)

	for _, server := range s.preCommit {
		t := time.Now()
		conn, err := s.FDialServer(ctx, server)
		if err != nil {
			return nil, status.Errorf(status.Convert(err).Code(), "Unable to dial %v -> %v", server, err)
		}
		defer conn.Close()

		client := pbrc.NewClientUpdateServiceClient(conn)
		_, err = client.ClientUpdate(ctx, &pbrc.ClientUpdateRequest{InstanceId: request.InstanceId})
		errors.With(prometheus.Labels{"client": server, "code": fmt.Sprintf("%v", status.Convert(err).Code())}).Inc()
		if err != nil {
			return nil, status.Errorf(status.Convert(err).Code(), "Unable to update %v -> %v", server, err)
		}
		preLatency.With(prometheus.Labels{"method": server}).Observe(float64(time.Since(t).Milliseconds()))
		serverTime[server] = time.Since(t)
	}

	t := time.Now()
	conn, err = s.FDialServer(ctx, "recordcollection")
	if err != nil {
		return nil, status.Errorf(status.Convert(err).Code(), "Unable to dial %v -> %v", "recordcollection", err)
	}
	defer conn.Close()

	rcclient := pbrc.NewRecordCollectionServiceClient(conn)
	_, err = rcclient.CommitRecord(ctx, &pbrc.CommitRecordRequest{InstanceId: request.InstanceId})
	errors.With(prometheus.Labels{"client": "commit", "code": fmt.Sprintf("%v", status.Convert(err).Code())}).Inc()

	if err != nil {
		return nil, err
	}
	serverTime["commit"] = time.Since(t)
	commitLatency.Observe(float64(time.Since(t).Milliseconds()))

	for _, server := range s.postCommit {
		t := time.Now()
		conn, err := s.FDialServer(ctx, server)
		if err != nil {
			return nil, status.Errorf(status.Convert(err).Code(), "Unable to dial %v -> %v", server, err)
		}
		defer conn.Close()

		client := pbrc.NewClientUpdateServiceClient(conn)
		_, err = client.ClientUpdate(ctx, &pbrc.ClientUpdateRequest{InstanceId: request.InstanceId})
		errors.With(prometheus.Labels{"client": server, "code": fmt.Sprintf("%v", status.Convert(err).Code())}).Inc()
		if err != nil {
			return nil, status.Errorf(status.Convert(err).Code(), "Unable to update %v -> %v", server, err)
		}
		postLatency.With(prometheus.Labels{"method": server}).Observe(float64(time.Since(t).Milliseconds()))
		serverTime[server] = time.Since(t)
	}

	longest := ""
	longestTime := time.Nanosecond
	for key, value := range serverTime {
		if value > longestTime {
			longest = key
			longestTime = value
		}
	}
	s.CtxLog(ctx, fmt.Sprintf("LongestTook %v -> %v", longestTime, longest))

	/*if time.Since(ot).Seconds() > 60 {
		key, _ := utils.GetContextKey(ctx)
		times := ""
		for key, value := range serverTime {
			if value > time.Second {
				times += fmt.Sprintf("%v took %v\n", key, value)
			}
		}
		//s.RaiseIssue("Slow fanout", fmt.Sprintf("Fanout for %v took %v (%v\n%v)", request.GetInstanceId(), time.Since(ot), key, times))
	}*/

	return &pb.FanoutResponse{}, nil
}
