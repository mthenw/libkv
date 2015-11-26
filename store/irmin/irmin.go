package irmin

import (
	"crypto/tls"
	"errors"
	"net/url"
	"sync"
	"time"

	"github.com/MagnusS/irmin-go/irmin"
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/hashicorp/consul/api"
)

var (
	// ErrMultipleEndpointsUnsupported is thrown when there are
	// multiple endpoints specified for Irmin
	ErrMultipleEndpointsUnsupported = errors.New("irmin does not support multiple endpoints")
)

// Irmin is the receiver type for the
// Store interface
type Irmin struct {
	sync.Mutex
	client *irmin.Conn
}

type irminLock struct {
	lock    *api.Lock
	renewCh chan struct{}
}

// Register registers irmin to libkv
func Register() {
	libkv.AddStore(store.IRMIN, New)
}

// New creates a new Irmin client given a list
// of endpoints and optional tls config
func New(endpoints []string, options *store.Config) (store.Store, error) {
	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}

	s := &Irmin{}

	uri, err := url.Parse("http://" + endpoints[0])
	if err != nil {
		return nil, err
	}

	// Set options
	if options != nil {
		if options.TLS != nil {
			s.setTLS(options.TLS)
		}
		if options.ConnectionTimeout != 0 {
			s.setTimeout(options.ConnectionTimeout)
		}
	}

	// Creates a new client
	client := irmin.Create(uri, "api-tester")
	s.client = client

	// Test get version
	_, err = s.client.Version()
	if err != nil {
		return nil, err
	}

	return s, nil
}

// SetTLS sets Irmin TLS options
func (s *Irmin) setTLS(tls *tls.Config) {
	// TODO
}

// SetTimeout sets the timeout for connecting to Irmin
func (s *Irmin) setTimeout(time time.Duration) {
	// TODO
}

// Normalize the key for usage in Irmin
func (s *Irmin) normalize(key string) string {
	key = store.Normalize(key)
	return key
}

// Get the value at "key", returns the last modified index
// to use in conjunction to CAS calls
func (s *Irmin) Get(key string) (*store.KVPair, error) {
	value, err := s.client.Read(irmin.ParsePath(s.normalize(key)))
	if err != nil {
		return nil, store.ErrKeyNotFound
	}

	return &store.KVPair{Key: key, Value: value}, nil
}

// Put a value at "key"
func (s *Irmin) Put(key string, value []byte, opts *store.WriteOptions) error {
	key = s.normalize(key)

	if opts != nil && opts.TTL > 0 {
		// TODO TTL ?
	}

	_, err := s.client.Update(s.client.NewTask("update key: "+key), irmin.ParsePath(key), value)
	return err
}

// Delete a "key" and all its subtree
func (s *Irmin) Delete(key string) error {
	key = s.normalize(key)
	err := s.client.RemoveRec(s.client.NewTask("delete key: "+key), irmin.ParsePath(key))
	return err
}

// Exists checks that the key exists inside the store
func (s *Irmin) Exists(key string) (bool, error) {
	_, err := s.Get(key)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List child nodes of a given directory
func (s *Irmin) List(directory string) ([]*store.KVPair, error) {
	directory = s.normalize(directory)

	paths, err := s.client.List(irmin.ParsePath(directory))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, store.ErrKeyNotFound
	}

	kv := []*store.KVPair{}

	for _, k := range paths {
		pair, err := s.Get(k.String())
		if err != nil {
			return nil, err
		}
		kv = append(kv, pair)
	}

	return kv, nil
}

// DeleteTree deletes a range of keys under a given directory
// For Irmin, it has the same effect than the regular 'Delete'
func (s *Irmin) DeleteTree(directory string) error {
	directory = s.normalize(directory)
	err := s.client.RemoveRec(s.client.NewTask("delete tree: "+directory), irmin.ParsePath(directory))
	return err
}

// Watch for changes on a "key"
// It returns a channel that will receive changes or pass
// on errors. Upon creation, the current value will first
// be sent to the channel. Providing a non-nil stopCh can
// be used to stop watching.
func (s *Irmin) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	eventCh, err := s.client.Watch(irmin.ParsePath(s.normalize(key)), nil)
	if err != nil {
		return nil, err
	}

	watchCh := make(chan *store.KVPair)

	go func() {
		defer close(watchCh)

		// Get the current value
		pair, err := s.Get(key)
		if err != nil {
			return
		}

		// Push the current value through the channel.
		watchCh <- pair

		for {
			select {
			case <-stopCh:
				// Stop watching
				return
			case c := <-eventCh:
				if c.Value != nil {
					watchCh <- &store.KVPair{
						Key:   key,
						Value: c.Value,
					}
				}
			}
		}
	}()

	return watchCh, nil
}

// WatchTree watches for changes on a "directory"
// It returns a channel that will receive changes or pass
// on errors. Upon creating a watch, the current childs values
// will be sent to the channel .Providing a non-nil stopCh can
// be used to stop watching.
func (s *Irmin) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	eventCh, err := s.client.WatchPath(irmin.ParsePath(s.normalize(directory)), nil)
	if err != nil {
		return nil, err
	}

	watchCh := make(chan []*store.KVPair)

	go func() {
		defer close(watchCh)

		for {
			select {
			case <-stopCh:
				// Stop watching
				return
			case e := <-eventCh:
				kvpairs := []*store.KVPair{}
				for _, c := range e.Changes {
					pair, err := s.Get(c.Key.String())
					if err != nil {
						return
					}
					kvpairs = append(kvpairs, &store.KVPair{
						Key:   pair.Key,
						Value: pair.Value,
					})
				}
				watchCh <- kvpairs
			}
		}
	}()

	return watchCh, nil
}

// NewLock returns a handle to a lock struct which can
// be used to provide mutual exclusion on a key
func (s *Irmin) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	// TODO
	return nil, store.ErrCallNotSupported
}

// Lock attempts to acquire the lock and blocks while
// doing so. It returns a channel that is closed if our
// lock is lost or if an error occurs
func (l *irminLock) Lock(stopChan chan struct{}) (<-chan struct{}, error) {
	// TODO
	return nil, store.ErrCallNotSupported
}

// Unlock the "key". Calling unlock while
// not holding the lock will throw an error
func (l *irminLock) Unlock() error {
	// TODO
	return store.ErrCallNotSupported
}

// AtomicPut put a value at "key" if the key has not been
// modified in the meantime, throws an error if this is the case
func (s *Irmin) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	// TODO
	return false, nil, store.ErrCallNotSupported
}

// AtomicDelete deletes a value at "key" if the key has not
// been modified in the meantime, throws an error if this is the case
func (s *Irmin) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	// TODO
	return false, store.ErrCallNotSupported
}

// Close closes the client connection
func (s *Irmin) Close() {
	return
}
