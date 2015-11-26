package irmin

import (
	"testing"
	"time"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	client = "localhost:6744"
)

func makeIrminClient(t *testing.T) store.Store {
	kv, err := New(
		[]string{client},
		&store.Config{
			ConnectionTimeout: 3 * time.Second,
		},
	)

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
}

func TestRegister(t *testing.T) {
	Register()

	kv, err := libkv.NewStore(store.IRMIN, []string{client}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*Irmin); !ok {
		t.Fatal("Error registering and initializing irmin")
	}
}

func TestIrminStore(t *testing.T) {
	kv := makeIrminClient(t)

	testutils.RunTestCommon(t, kv)
	// testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	// testutils.RunTestLock(t, kv)
	// testutils.RunTestLockTTL(t, kv, lockKV)
	// testutils.RunTestTTL(t, kv, ttlKV)
	testutils.RunCleanup(t, kv)
}
