package proximoc

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	"github.com/utilitywarehouse/proximo/go-proximo"
)

func ConsumeContext(ctx context.Context, proximoAddress string, consumer string, topic string, f func(*proximo.Message) error) error {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

	conn, err := grpc.Dial(proximoAddress, opts...)
	if err != nil {
		grpclog.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := proximo.NewMessageSourceClient(conn)

	stream, err := client.Consume(ctx)
	if err != nil {
		grpclog.Fatalf("%v.Consume(_) = _, %v", client, err)
	}

	defer stream.CloseSend()

	handled := make(chan string)
	errs := make(chan error, 2)

	var wg sync.WaitGroup
	defer wg.Wait()

	ins := make(chan *proximo.Message, 16) // TODO: make buffer size configurable?

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(ins)
		for {
			in, err := stream.Recv()
			if err != nil {
				if err != io.EOF && grpc.Code(err) != 1 { // 1 means cancelled
					errs <- err
				}
				return
			}
			ins <- in
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case in, ok := <-ins:
				if !ok {
					return
				}
				if err := f(in); err != nil {
					errs <- err
					return
				}
				select {
				case handled <- in.GetId():
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	if err := stream.Send(&proximo.Request{
		StartRequest: &proximo.StartRequest{
			Topic:    topic,
			Consumer: consumer,
		},
	}); err != nil {
		return err
	}

	for {
		select {
		case id := <-handled:
			if err := stream.Send(&proximo.Request{Confirmation: &proximo.Confirmation{MsgID: id}}); err != nil {
				return err
			}
		case err := <-errs:
			return err
		case <-ctx.Done():
			return nil //ctx.Err()
		}

	}

}
