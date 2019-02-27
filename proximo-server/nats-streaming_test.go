package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Shopify/toxiproxy"
	stan "github.com/nats-io/go-nats-streaming"
	stand "github.com/nats-io/nats-streaming-server/server"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestConsumerErrorOnBackendDisconnect(t *testing.T) {
	logrus.StandardLogger().Out = ioutil.Discard

	// seed nats with some test data
	stanServerOpts := stand.GetDefaultOptions()
	natsServerOpts := stand.DefaultNatsServerOptions
	natsServerOpts.Port = 10247 // sorry!
	natsServ, err := stand.RunServerWithOpts(stanServerOpts, &natsServerOpts)
	require.NoError(t, err)
	defer natsServ.Shutdown()
	conn, err := stan.Connect(stand.DefaultClusterID, "test-publish", stan.NatsURL(fmt.Sprintf("nats://localhost:%d", natsServerOpts.Port)))
	require.NoError(t, err)
	for i := 0; i < 1000; i++ {
		err := conn.Publish("test", []byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	// set up backend handler with a proxy in the connection
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	proxy := toxiproxy.NewProxy()
	proxy.Listen = "localhost:10248"
	proxy.Upstream = fmt.Sprintf("localhost:%d", natsServerOpts.Port)
	err = proxy.Start()
	hnd, err := newNatsStreamingConsumeHandler(
		fmt.Sprintf("nats://%s", proxy.Listen), stand.DefaultClusterID, 1, 1, 3)
	require.NoError(t, err)
	success := make(chan struct{})
	egrp, groupCtx := errgroup.WithContext(ctx)
	forClient := make(chan *Message)
	acks := make(chan *Confirmation)
	egrp.Go(func() error {
		for msg := range forClient {
			val, _ := strconv.Atoi(string(msg.Data))
			if val == 10 {
				t.Log("close proxy after 10 msgs")
				proxy.Stop() // close proxy after 10 msgs
			}
			acks <- &Confirmation{MsgID: msg.Id}
		}
		return nil
	})
	egrp.Go(func() error {
		err := hnd.HandleConsume(groupCtx, consumerConfig{"test", "test"}, forClient, acks)
		if err != nil && err != context.Canceled {
			t.Log(err)
			close(success) // our handler returned the error from the ping timeout
		}
		return err
	})
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-success:
	}
}
