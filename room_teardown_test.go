package main

import (
	"testing"
	"time"
)

// Creator hangs up the PSTN leg (SIP BYE) → the fan must receive the
// terminal signal. Regression: SendSignalingMessage dropped everything
// once room status was Ended, and handleBye set Ended BEFORE sending, so
// the fan's call never dropped until the backend webhook round-tripped.
func TestCloseDeliversLeftBeforeTeardown(t *testing.T) {
	room := &Room{
		ID:            "room_test_left",
		Status:        RoomStatusConnected,
		SignalingChan: make(chan SignalingMessage, 100),
	}

	received := make(chan SignalingMessage, 10)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for msg := range room.SignalingChan {
			received <- msg
		}
	}()

	room.Close()

	select {
	case msg := <-received:
		if msg.Type != "left" {
			t.Fatalf("expected first teardown message to be left, got %q", msg.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for left message on room Close")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("signaling channel was not closed by room Close")
	}
}

// A room already Ended (BYE path delivers its own `left` before flipping
// status) must not deliver a second terminal signal from Close.
func TestCloseSkipsLeftWhenAlreadyEnded(t *testing.T) {
	room := &Room{
		ID:            "room_test_ended",
		Status:        RoomStatusEnded,
		SignalingChan: make(chan SignalingMessage, 100),
	}

	drained := make(chan int, 1)
	go func() {
		n := 0
		for range room.SignalingChan {
			n++
		}
		drained <- n
	}()

	room.Close()

	select {
	case n := <-drained:
		if n != 0 {
			t.Fatalf("expected no messages on Close of an ended room, got %d", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("signaling channel was not closed by room Close")
	}
}
