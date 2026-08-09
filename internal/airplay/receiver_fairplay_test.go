package airplay

import (
	"bytes"
	"testing"
)

func TestReceiverFPSAPCompletesSenderExchange(t *testing.T) {
	sender, err := newFPSAPSession(bytes.NewReader(bytes.Repeat([]byte{0x31}, 126)))
	if err != nil {
		t.Fatalf("create sender FPSAP state: %v", err)
	}
	receiver, err := newReceiverFPSAPState(bytes.NewReader(bytes.Repeat([]byte{0x72}, 126)))
	if err != nil {
		t.Fatalf("create receiver FPSAP state: %v", err)
	}

	m2, err := receiver.exchange(sender.message1())
	if err != nil {
		t.Fatalf("exchange m2: %v", err)
	}
	m3, err := sender.exchangeM3(m2)
	if err != nil {
		t.Fatalf("exchange m3: %v", err)
	}
	m4, err := receiver.exchange(m3)
	if err != nil {
		t.Fatalf("exchange m4: %v", err)
	}
	if err := sender.confirmM4(m4); err != nil {
		t.Fatalf("confirm m4: %v", err)
	}
	if !receiver.complete() {
		t.Fatal("receiver did not mark FPSAP exchange complete")
	}
}

func TestReceiverFPSAPRejectsMalformedAndOutOfOrderMessages(t *testing.T) {
	newState := func(t *testing.T) *receiverFPSAPState {
		t.Helper()
		state, err := newReceiverFPSAPState(bytes.NewReader(bytes.Repeat([]byte{0x44}, 126)))
		if err != nil {
			t.Fatalf("create receiver FPSAP state: %v", err)
		}
		return state
	}

	if _, err := newState(t).exchange([]byte("not an FPSAP record")); err == nil {
		t.Fatal("receiver accepted malformed m1")
	}

	sender, err := newFPSAPSession(bytes.NewReader(bytes.Repeat([]byte{0x55}, 126)))
	if err != nil {
		t.Fatal(err)
	}
	receiver := newState(t)
	m2, err := receiver.exchange(sender.message1())
	if err != nil {
		t.Fatal(err)
	}
	m3, err := sender.exchangeM3(m2)
	if err != nil {
		t.Fatal(err)
	}
	m3[len(m3)-1] ^= 1
	if _, err := receiver.exchange(m3); err == nil {
		t.Fatal("receiver accepted an invalid m3 confirmation")
	}

	receiver = newState(t)
	m2, err = receiver.exchange(sender.message1())
	if err != nil {
		t.Fatal(err)
	}
	m3, err = sender.exchangeM3(m2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.exchange(m3); err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.exchange(m3); err == nil {
		t.Fatal("receiver accepted a second m3 after completion")
	}
}
