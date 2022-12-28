package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/brotherlogic/goserver/utils"

	pbrc "github.com/brotherlogic/recordcollection/proto"
	pb "github.com/brotherlogic/recordfanout/proto"
)

func main() {
	ctx, cancel := utils.ManualContext("recordfanout-cli", time.Hour*5)
	defer cancel()

	conn, err := utils.LFDialServer(ctx, "recordfanout")
	if err != nil {
		log.Fatalf("Unable to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewRecordFanoutServiceClient(conn)

	switch os.Args[1] {
	case "ping":
		fanoutFlags := flag.NewFlagSet("Fanout", flag.ExitOnError)
		var id = fanoutFlags.Int("id", -1, "Id of the record to add")

		if err := fanoutFlags.Parse(os.Args[2:]); err == nil {
			res, err := client.Fanout(ctx, &pb.FanoutRequest{InstanceId: int32(*id)})
			fmt.Printf("Fanout response: %v/%v\n", res, err)
		}
	case "clean":
		conn, err := utils.LFDialServer(ctx, "recordcollection")
		if err != nil {
			log.Fatalf("Unable to dial: %v", err)
		}
		c2 := pbrc.NewRecordCollectionServiceClient(conn)
		recs, err := c2.QueryRecords(ctx, &pbrc.QueryRecordsRequest{Query: &pbrc.QueryRecordsRequest_UpdateTime{0}})
		for _, rec := range recs.GetInstanceIds() {
			r, err := c2.GetRecord(ctx, &pbrc.GetRecordRequest{InstanceId: rec})
			if err != nil {
				log.Fatalf("Unable to get: %v", err)
			}
			if r.GetRecord().GetMetadata().GetDirty() {
				fmt.Printf("Cleaning %v\n", r.GetRecord().GetRelease().GetTitle())
				client.Fanout(ctx, &pb.FanoutRequest{InstanceId: rec})
			}
		}
	case "recache":
		conn, err := utils.LFDialServer(ctx, "recordcollection")
		if err != nil {
			log.Fatalf("Unable to dial: %v", err)
		}
		c2 := pbrc.NewRecordCollectionServiceClient(conn)
		recs, err := c2.QueryRecords(ctx, &pbrc.QueryRecordsRequest{Query: &pbrc.QueryRecordsRequest_UpdateTime{0}})
		for _, rec := range recs.GetInstanceIds() {
			r, err := c2.GetRecord(ctx, &pbrc.GetRecordRequest{InstanceId: rec})
			if err != nil {
				log.Fatalf("Unable to get: %v", err)
			}
			if time.Since(time.Unix(r.GetRecord().GetMetadata().GetLastCache(), 0)) > time.Hour*24*30 {
				fmt.Printf("Recaching %v\n", r.GetRecord().GetRelease().GetTitle())
				client.Fanout(ctx, &pb.FanoutRequest{InstanceId: rec})
			}
		}
	case "fullping":
		conn, err := utils.LFDialServer(ctx, "recordcollection")
		if err != nil {
			log.Fatalf("Unable to dial: %v", err)
		}
		c2 := pbrc.NewRecordCollectionServiceClient(conn)
		recs, err := c2.QueryRecords(ctx, &pbrc.QueryRecordsRequest{Query: &pbrc.QueryRecordsRequest_UpdateTime{0}})
		if err == nil {
			log.Printf("READ %v recordds", len(recs.GetInstanceIds()))
			for _, rec := range recs.GetInstanceIds() {
				_, err := client.Fanout(ctx, &pb.FanoutRequest{InstanceId: rec})
				log.Printf("FANOUT %v -> %v", rec, err)
			}
		}
	}

}
