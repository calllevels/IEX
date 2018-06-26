package main

import (
	"context"
	"io"
	"log"
	"time"

	pb "github.com/calllevels/data-stream/datasource"
	grpc "google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("localhost:10000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewDataStreamClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err != nil {
		log.Fatalf("cannot create message: %v", err)
	}
	if err != nil {
		log.Fatalf("cannot receive message: %v", err)
	}
	stream, err := c.GetMessage(ctx, &pb.MessageRequest{})
	for {
		d, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("%v.GetMessage(_) = _ %v", c, err)
		}
		log.Println(d)
	}
}
