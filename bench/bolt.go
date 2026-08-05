//go:build linux

package main

import (
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

var logsBucket = []byte("logs")

// boltStore is the B-tree alternative: the shape hashicorp/raft-boltdb has
// shipped for years. One bbolt transaction per batch, and bbolt fsyncs on
// commit by default, so its durability promise matches the segment arms.
type boltStore struct {
	db    *bolt.DB
	first uint64
	last  uint64
	stats pagerStats
}

func newBoltStore(path string) (*boltStore, error) {
	db, err := bolt.Open(path, 0o644, &bolt.Options{
		// NoSync stays false: every commit is durable, same as the other arms.
		FreelistType: bolt.FreelistMapType,
	})
	if err != nil {
		return nil, err
	}
	s := &boltStore{db: db, first: 1}
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(logsBucket)
		if err != nil {
			return err
		}
		c := b.Cursor()
		if k, _ := c.First(); k != nil {
			s.first = binary.BigEndian.Uint64(k)
		}
		if k, _ := c.Last(); k != nil {
			s.last = binary.BigEndian.Uint64(k)
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func key(i uint64) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], i)
	return k[:]
}

func (s *boltStore) Append(entries []Entry) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(logsBucket)
		for _, e := range entries {
			// A fresh buffer per entry: bbolt keeps a reference to the value
			// until the transaction commits, so reusing one buffer would make
			// every key in the batch alias the last entry written.
			buf := make([]byte, HeaderSize+len(e.Payload))
			// Identical framing to the segment arms, so the comparison is not
			// distorted by a different serialization format.
			n := EncodeEntry(e, buf)
			if err := b.Put(key(e.Index), buf[:n]); err != nil {
				return err
			}
			s.stats.PayloadBytes += int64(n)
			s.last = e.Index
		}
		s.stats.SyncCalls++
		return nil
	})
}

func (s *boltStore) Get(index uint64) (Entry, error) {
	var out Entry
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(logsBucket).Get(key(index))
		if v == nil {
			return fmt.Errorf("index %d not found", index)
		}
		e, _, err := DecodeEntry(v, 1<<24)
		out = e
		return err
	})
	return out, err
}

func (s *boltStore) TruncateTail(lastIndex uint64) error {
	if lastIndex >= s.last {
		return nil
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(logsBucket)
		c := b.Cursor()
		// Every dropped key is a B-tree delete that dirties pages and forces
		// free-space bookkeeping: precisely the cost segment unlink avoids.
		for k, _ := c.Seek(key(lastIndex + 1)); k != nil; k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.last = lastIndex
	return nil
}

func (s *boltStore) FirstIndex() uint64 { return s.first }
func (s *boltStore) LastIndex() uint64  { return s.last }

func (s *boltStore) Stats() pagerStats {
	st := s.stats
	bs := s.db.Stats()
	st.DeviceWrites = int64(bs.TxStats.GetWrite())
	return st
}

func (s *boltStore) Close() error { return s.db.Close() }
