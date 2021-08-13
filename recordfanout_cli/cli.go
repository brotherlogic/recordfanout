package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/brotherlogic/goserver/utils"

	pb "github.com/brotherlogic/recordfanout/proto"

	//Needed to pull in gzip encoding init
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/resolver"
)

func init() {
	resolver.Register(&utils.DiscoveryClientResolverBuilder{})
}

func main() {
	ctx, cancel := utils.ManualContext("recordfanout-cli", time.Minute*5)
	defer cancel()

	conn, err := utils.LFDialServer(ctx, "recordfanout")
	if err != nil {
		log.Fatalf("Unable to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewRecordFanoutServiceClient(conn)

	fanoutFlags := flag.NewFlagSet("Fanout", flag.ExitOnError)
	var id = fanoutFlags.Int("id", -1, "Id of the record to add")

	if err := fanoutFlags.Parse(os.Args[1:]); err == nil {
		res, err := client.Fanout(ctx, &pb.FanoutRequest{InstanceId: int32(*id)})
		fmt.Printf("Fanout response: %v/%v\n", res, err)
	}

}
