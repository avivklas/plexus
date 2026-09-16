package graph

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/avivklas/plexus/pkg/dedup"
	"github.com/avivklas/plexus/pkg/machine"
	"github.com/avivklas/plexus/pkg/store"
)

// AttachDedupInterceptor wraps all handlers on a downstream Store to enforce Exactly-Once Semantics (EOS)
// using the provided dedup.Store.
func AttachDedupInterceptor(s store.Store, dedupStore dedup.Store) {
	router := s.Router()
	handlers := router.Handlers()

	for cmdType, originalHandler := range handlers {
		currHandler := originalHandler
		wrapped := func(ctx context.Context, data []byte) (any, error) {
			var idempKey string
			if cmd, ok := ctx.Value(machine.CtxKeyCommand).(*store.Command); ok && cmd != nil {
				idempKey = cmd.IdempotencyKey()
			}
			if idempKey == "" {
				if v, ok := ctx.Value(store.MetadataKeyIdempotency).(string); ok {
					idempKey = v
				}
			}

			if idempKey != "" {
				rec, found, err := dedupStore.Check(idempKey)
				if err != nil {
					return nil, fmt.Errorf("dedup check failed: %w", err)
				}
				if found {
					// Duplicate detected! Return cached response without re-applying mutation.
					var cachedRes any
					if len(rec.Result) > 0 {
						_ = json.Unmarshal(rec.Result, &cachedRes)
					}
					return cachedRes, nil
				}
			}

			// Apply mutation
			res, err := currHandler(ctx, data)
			if err != nil {
				return nil, err
			}

			// Record success in dedup store
			if idempKey != "" {
				var resBytes []byte
				if res != nil {
					resBytes, _ = json.Marshal(res)
				}

				var logIndex, logTerm uint64
				if v, ok := ctx.Value(machine.CtxKeyLogIndex).(uint64); ok {
					logIndex = v
				}
				if v, ok := ctx.Value(machine.CtxKeyLogTerm).(uint64); ok {
					logTerm = v
				}

				_ = dedupStore.Record(dedup.Record{
					IdempotencyKey: idempKey,
					Index:          logIndex,
					Term:           logTerm,
					Result:         resBytes,
				})
			}

			return res, nil
		}
		router.Handle(cmdType, wrapped)
	}
}
