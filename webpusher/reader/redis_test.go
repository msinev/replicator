// +build go1.7

package reader

import (
	"context"
	"fmt"
	"testing"
)

func serverAddr() string {
	return "127.0.0.1:6379"
}

func TestPlainDB(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	db := 0
	t.Logf("Using %s DB %d for testing", serverAddr(), db)
	err := listenPubSubChannels(ctx,
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
		chan_db_list(db, nil))

	if err != nil {
		t.Log(err)
		t.Failed()
		return
	}

}
