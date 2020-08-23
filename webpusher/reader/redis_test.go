// +build go1.7

package reader

import (
	"context"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strconv"
	"testing"
	"time"
)

func serverAddr() string {
	return "127.0.0.1:6379"
}

func TestPlainDB(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Log("Send REDIS commands now!\nChange value My to exit")

	db := 0
	t.Logf("Using %s DB %d for testing", serverAddr(), db)
	err := ListenPubSubChannelsTest(ctx,
		serverAddr(),
		func() error {
			// The start callback is a good place to backfill missed
			// notifications. For the purpose of this example, a goroutine is
			// started to send notifications.

			return nil
		},
		func(channel string, message []byte) error {
			fmt.Printf("channel: %s, message: %s\n", channel, message)

			// For the purpose of this example, cancel the listener's context
			// after receiving last message sent by publish().
			if string(message) == "My" {
				cancel()
			}
			return nil
		},
		Chan_db_list(db, nil))

	if err != nil {
		t.Log(err)
		t.Failed()
		return
	}

}
func Chan_db_list(db int, pre []string) []string {
	dbs := strconv.Itoa(db)
	return command_list("__keyevent@"+dbs+"__:", pre)
}

func command_list(prefix string, pre []string) []string {

	for _, v := range events {
		pre = append(pre, prefix+v)
	}
	log.Info(pre)
	return pre
}

// Copyright 2012 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// listenPubSubChannels listens for messages on Redis pubsub channels. The
// onStart function is called after the channels are subscribed. The onMessage
// function is called for each message.

func ListenPubSubChannelsTest(ctx context.Context, redisServerAddr string,
	onStart func() error,
	onMessage func(channel string, data []byte) error,
	channels []string) error {
	// A ping is set to the server with this period to test for the health of
	// the connection and server.
	const healthCheckPeriod = time.Minute

	c, err := redis.Dial("tcp", redisServerAddr,
		// Read timeout on server should be greater than ping period.
		redis.DialReadTimeout(healthCheckPeriod+10*time.Second),
		redis.DialWriteTimeout(10*time.Second))
	if err != nil {
		return err
	}
	defer c.Close()

	psc := redis.PubSubConn{Conn: c}

	if err := psc.Subscribe(redis.Args{}.AddFlat(channels)...); err != nil {
		return err
	}

	done := make(chan error, 1)

	// Start a goroutine to receive notifications from the server.
	go func() {
		for {
			switch n := psc.Receive().(type) {
			case error:
				done <- n
				return
			case redis.Message:
				if err := onMessage(n.Channel, n.Data); err != nil {
					done <- err
					return
				}
			case redis.Subscription:
				switch n.Count {
				case len(channels):
					// Notify application when all channels are subscribed.
					if err := onStart(); err != nil {
						done <- err
						return
					}
				case 0:
					// Return from the goroutine when all channels are unsubscribed.
					done <- nil
					return
				}
			}
		}
	}()

	ticker := time.NewTicker(healthCheckPeriod)
	defer ticker.Stop()
loop:
	for err == nil {
		select {
		case <-ticker.C:
			// Send ping to test health of connection and server. If
			// corresponding pong is not received, then receive on the
			// connection will timeout and the receive goroutine will exit.
			if err = psc.Ping(""); err != nil {
				break loop
			}
		case <-ctx.Done():
			break loop
		case err := <-done:
			// Return error from the receive goroutine.
			return err
		}
	}

	// Signal the receiving goroutine to exit by unsubscribing from all channels.
	psc.Unsubscribe()

	// Wait for goroutine to complete.
	return <-done
}
