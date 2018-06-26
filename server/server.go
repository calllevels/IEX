package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	pb "github.com/calllevels/data-stream/datasource"
	grpc "google.golang.org/grpc"
)

type Symbol struct {
	Symbol string `json:"symbol"`
}

type OHLC struct {
	Open  PriceTime `json:"Open"`
	Close PriceTime `json:"Close"`
	High  float64   `json:"High"`
	Low   float64   `json:"Low"`
}

type PriceTime struct {
	Price float64 `json:"Price"`
	Time  int64   `json:"Time"`
}

type server struct {
	messageResponses []*pb.MessageResponse
	messageRequest   *pb.MessageRequest
}

func main() {
	port := flag.Int("port", 10000, "server port")
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterDataStreamServer(grpcServer, newServer())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func newServer() *server {
	s := &server{}
	s.initialize()
	return s
}

func (s *server) GetMessage(in *pb.MessageRequest, srv pb.DataStream_GetMessageServer) error {
	for _, res := range s.messageResponses {
		if err := srv.Send(res); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) initialize() error {
	symbol := getSymbols()

	var responses []*pb.MessageResponse
	for _, symbol := range symbol {
		strings.Trim(symbol.Symbol, "{}")
		stmt := fmt.Sprintf("https://api.iextrading.com/1.0/stock/%v/ohlc", symbol.Symbol)
		resp, err := http.Get(stmt)
		if err != nil {
			fmt.Println(err)
		}

		contents, err := ioutil.ReadAll(resp.Body)
		response := pb.MessageResponse{Type: symbol.Symbol, Data: contents}
		responses = append(responses, &response)
		defer resp.Body.Close()
	}
	s.messageResponses = responses
	return nil
}

func toJson(p interface{}) string {
	bytes, err := json.Marshal(p)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	return string(bytes)
}

func (p Symbol) toString() string {
	return toJson(p)
}

func getSymbols() []Symbol {
	raw, err := ioutil.ReadFile("./stocks.json")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	var c []Symbol
	json.Unmarshal(raw, &c)
	return c
}
