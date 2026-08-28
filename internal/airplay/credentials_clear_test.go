package airplay

import (
	"crypto/ed25519"
	"crypto/rand"
	"path/filepath"
	"testing"
)

func TestCredentialStoreClearRestoreTokenPreservesPairingAndOtherDevices(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json")
	store, err := NewCredentialStore(path)
	if err != nil {
		t.Fatalf("NewCredentialStore: %v", err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if err := store.SavePairing("device-1", "pair-1", pub, priv, PairingProtocolHAP); err != nil {
		t.Fatalf("SavePairing: %v", err)
	}
	if err := store.SaveRestoreToken("device-1", "restore-1"); err != nil {
		t.Fatalf("SaveRestoreToken device-1: %v", err)
	}
	if err := store.SaveRestoreToken("device-2", "restore-2"); err != nil {
		t.Fatalf("SaveRestoreToken device-2: %v", err)
	}

	if err := store.ClearRestoreToken("device-1"); err != nil {
		t.Fatalf("ClearRestoreToken: %v", err)
	}

	reloaded, err := NewCredentialStore(path)
	if err != nil {
		t.Fatalf("reload credential store: %v", err)
	}
	cleared := reloaded.Lookup("device-1")
	if cleared == nil || !cleared.HasPairingCredentials() {
		t.Fatal("clearing the restore token removed pairing credentials")
	}
	if cleared.PairingID != "pair-1" || cleared.PairingProtocol != PairingProtocolHAP {
		t.Fatalf("pairing metadata changed: %+v", cleared)
	}
	if cleared.RestoreToken != "" {
		t.Fatalf("restore token = %q, want empty", cleared.RestoreToken)
	}
	other := reloaded.Lookup("device-2")
	if other == nil || other.RestoreToken != "restore-2" {
		t.Fatalf("other device changed: %+v", other)
	}
}

type recordingCredentialBackend struct {
	devices map[string]*SavedCredentials
	saves   []string
}

func (b *recordingCredentialBackend) Lookup(deviceID string) (*SavedCredentials, error) {
	return b.devices[deviceID], nil
}

func (b *recordingCredentialBackend) Save(deviceID string, creds *SavedCredentials) error {
	b.devices[deviceID] = creds
	b.saves = append(b.saves, deviceID)
	return nil
}

func TestCredentialStoreClearRestoreTokenUsesBackendWithoutDeletingEntry(t *testing.T) {
	backend := &recordingCredentialBackend{devices: map[string]*SavedCredentials{
		"device-1": {PairingID: "pair-1", RestoreToken: "restore-1"},
	}}
	store := NewCredentialStoreWithBackend(backend)

	if err := store.ClearRestoreToken("device-1"); err != nil {
		t.Fatalf("ClearRestoreToken: %v", err)
	}

	if len(backend.saves) != 1 || backend.saves[0] != "device-1" {
		t.Fatalf("backend saves = %v, want [device-1]", backend.saves)
	}
	creds := backend.devices["device-1"]
	if creds == nil || creds.PairingID != "pair-1" || creds.RestoreToken != "" {
		t.Fatalf("backend credentials after clear = %+v", creds)
	}
}
