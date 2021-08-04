package main

import (
	"fmt"

	"github.com/brotherlogic/goserver"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	gspb "github.com/brotherlogic/goserver/proto"
)

const (
	CONFIG_KEY = "/github.com/brotherlogic/queues/config"
)

//Server main server type
type Server struct {
	*goserver.GoServer
}

// Init builds the server
func Init() *Server {
	s := &Server{
		GoServer: &goserver.GoServer{},
	}
	return s
}

// DoRegister does RPC registration
func (s *Server) DoRegister(server *grpc.Server) {

}

// ReportHealth alerts if we're not healthy
func (s *Server) ReportHealth() bool {
	return true
}

// Shutdown the server
func (s *Server) Shutdown(ctx context.Context) error {
	return nil
}

// Mote promotes/demotes this server
func (s *Server) Mote(ctx context.Context, master bool) error {
	return nil
}

// GetState gets the state of the server
func (s *Server) GetState() []*gspb.State {
	return []*gspb.State{}
}

func main() {
	server := Init()
	server.PrepServer()
	server.Register = server

	err := server.RegisterServerV2("recordfanout", false, true)
	if err != nil {
		return
	}

	fmt.Printf("%v", server.Serve())
}
