package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// CommandHandler executes a state mutation on raw payload.
type CommandHandler func(ctx context.Context, data []byte) (any, error)

// PreHook is middleware executed before a command is applied.
type PreHook func(ctx context.Context, cmd *Command) (*Command, error)

// PostHook is middleware executed after a command has been applied.
type PostHook func(ctx context.Context, cmd *Command, applyRes any, applyErr error) (any, error)

// Router maps CommandType to handlers and manages middleware.
type Router struct {
	mu        sync.RWMutex
	handlers  map[CommandType]CommandHandler
	preHooks  map[CommandType]PreHook
	postHooks map[CommandType]PostHook
}

// NewRouter creates an empty command router.
func NewRouter() *Router {
	return &Router{
		handlers:  make(map[CommandType]CommandHandler),
		preHooks:  make(map[CommandType]PreHook),
		postHooks: make(map[CommandType]PostHook),
	}
}

// Handle registers a raw CommandHandler for the given CommandType.
func (r *Router) Handle(cmdType CommandType, h CommandHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[cmdType] = h
}

// HandleTyped registers a generic, type-safe handler for the given CommandType.
// It automatically unmarshals JSON payloads into Req and marshals/returns Resp.
func HandleTyped[Req any, Resp any](
	r *Router,
	cmdType CommandType,
	fn func(ctx context.Context, req Req) (Resp, error),
) {
	r.Handle(cmdType, func(ctx context.Context, data []byte) (any, error) {
		var req Req
		if len(data) > 0 {
			if err := json.Unmarshal(data, &req); err != nil {
				return nil, fmt.Errorf("unmarshal request for %s: %w", cmdType, err)
			}
		}
		return fn(ctx, req)
	})
}

// GetHandler returns the registered handler for the given CommandType.
func (r *Router) GetHandler(cmdType CommandType) (CommandHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[cmdType]
	return h, ok
}

// Handlers returns a shallow copy of all registered handlers.
func (r *Router) Handlers() map[CommandType]CommandHandler {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copyMap := make(map[CommandType]CommandHandler, len(r.handlers))
	for k, v := range r.handlers {
		copyMap[k] = v
	}
	return copyMap
}

// AddPreHook adds a pre-hook for a specific command type.
func (r *Router) AddPreHook(cmdType CommandType, hook PreHook) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.preHooks[cmdType] = hook
}

// AddPostHook adds a post-hook for a specific command type.
func (r *Router) AddPostHook(cmdType CommandType, hook PostHook) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.postHooks[cmdType] = hook
}

// PreHooks returns a copy of registered pre-hooks.
func (r *Router) PreHooks() map[CommandType]PreHook {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copyMap := make(map[CommandType]PreHook, len(r.preHooks))
	for k, v := range r.preHooks {
		copyMap[k] = v
	}
	return copyMap
}

// PostHooks returns a copy of registered post-hooks.
func (r *Router) PostHooks() map[CommandType]PostHook {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copyMap := make(map[CommandType]PostHook, len(r.postHooks))
	for k, v := range r.postHooks {
		copyMap[k] = v
	}
	return copyMap
}

// Execute runs the registered handler and pre/post hooks for the command.
func (r *Router) Execute(ctx context.Context, cmd *Command) (any, error) {
	if cmd == nil {
		return nil, fmt.Errorf("nil command")
	}

	r.mu.RLock()
	preHook := r.preHooks[cmd.Type]
	postHook := r.postHooks[cmd.Type]
	handler, ok := r.handlers[cmd.Type]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("no handler registered for command type %q", cmd.Type)
	}

	if preHook != nil {
		modifiedCmd, hookErr := preHook(ctx, cmd)
		if hookErr != nil {
			return nil, fmt.Errorf("pre-hook error on %s: %w", cmd.Type, hookErr)
		}
		cmd = modifiedCmd
	}

	res, applyErr := handler(ctx, cmd.Data)

	if postHook != nil {
		return postHook(ctx, cmd, res, applyErr)
	}

	return res, applyErr
}
