package airplay

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
)

// receiverFPSAPState is the receiver half of the two-request FairPlay SAP
// exchange. It uses the same clean protocol primitives as the sender while
// keeping the direction and sequencing explicit.
type receiverFPSAPState struct {
	receiverSAP [128]byte
	mode        byte
	phase       byte
	m3          [164]byte
}

func newReceiverFPSAPState(entropy io.Reader) (*receiverFPSAPState, error) {
	if entropy == nil {
		entropy = rand.Reader
	}
	state := &receiverFPSAPState{mode: 3}
	state.receiverSAP[1] = 1
	if _, err := io.ReadFull(entropy, state.receiverSAP[2:]); err != nil {
		return nil, fmt.Errorf("initialize receiver SAP: %w", err)
	}
	return state, nil
}

func (s *receiverFPSAPState) exchange(request []byte) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("FairPlay SAP state is nil")
	}
	switch s.phase {
	case 0:
		return s.exchangeM2(request)
	case 1:
		return s.exchangeM4(request)
	default:
		return nil, fmt.Errorf("FairPlay SAP exchange is already complete")
	}
}

func (s *receiverFPSAPState) exchangeM2(m1 []byte) ([]byte, error) {
	if err := validateFPSAPRecord(m1, 1, len(fpsapM1Payload)); err != nil {
		return nil, fmt.Errorf("invalid m1: %w", err)
	}
	if !bytes.Equal(m1[12:], fpsapM1Payload[:]) {
		return nil, fmt.Errorf("unsupported m1 capabilities %x", m1[12:])
	}

	m2 := newFPSAPRecord(2, 130)
	m2[12] = 2
	m2[13] = s.mode
	if err := encryptFairPlayMessage(s.mode, s.receiverSAP[:], m2[14:142]); err != nil {
		return nil, fmt.Errorf("encrypt m2 SAP: %w", err)
	}
	s.phase = 1
	return m2, nil
}

func (s *receiverFPSAPState) exchangeM4(m3 []byte) ([]byte, error) {
	if err := validateFPSAPRecord(m3, 3, 152); err != nil {
		return nil, fmt.Errorf("invalid m3: %w", err)
	}
	if m3[12] != s.mode {
		return nil, fmt.Errorf("m3 mode %d does not match selected mode %d", m3[12], s.mode)
	}
	if !bytes.Equal(m3[13:16], fpsapM3Label[:]) {
		return nil, fmt.Errorf("invalid m3 label %x", m3[13:16])
	}

	var senderSAP [128]byte
	decryptFairPlayMessage(m3, senderSAP[:])
	wantConfirmation := fpsapExchangeForSAP(senderSAP, s.receiverSAP)
	if !bytes.Equal(m3[144:], wantConfirmation[:]) {
		return nil, fmt.Errorf("m3 exchange confirmation is invalid")
	}

	copy(s.m3[:], m3)
	s.phase = 2
	m4 := newFPSAPRecord(4, 20)
	copy(m4[12:], m3[144:])
	return m4, nil
}

func (s *receiverFPSAPState) complete() bool {
	return s != nil && s.phase == 2
}
