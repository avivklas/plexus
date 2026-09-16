package store

import (
	"encoding/json"
	"fmt"
)

// CommandType uniquely identifies a state mutation within a Store.
type CommandType string

func (c CommandType) String() string {
	return string(c)
}

const (
	// MetadataKeyIdempotency is the metadata key for graph deduplication.
	MetadataKeyIdempotency = "x-idempotency-key"
	// MetadataKeyOriginMachine is the machine ID that produced this command.
	MetadataKeyOriginMachine = "x-origin-machine"
	// MetadataKeyOriginTerm is the Raft term in which the command originated.
	MetadataKeyOriginTerm = "x-origin-term"
	// MetadataKeyOriginIndex is the Raft log index in which the command originated.
	MetadataKeyOriginIndex = "x-origin-index"
	// MetadataKeyOriginSeq is the sub-sequence number within the upstream command.
	MetadataKeyOriginSeq = "x-origin-seq"
)

// Command is the envelope passed to a state machine FSM.
type Command struct {
	Type     CommandType       `json:"type"`
	Data     []byte            `json:"data"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// NewCommand creates and serializes data into a Command.
func NewCommand(cmdType CommandType, data any) (*Command, error) {
	var payload []byte
	if data != nil {
		switch v := data.(type) {
		case []byte:
			payload = v
		case string:
			payload = []byte(v)
		default:
			b, err := json.Marshal(data)
			if err != nil {
				return nil, fmt.Errorf("marshal command payload: %w", err)
			}
			payload = b
		}
	}
	return &Command{
		Type:     cmdType,
		Data:     payload,
		Metadata: make(map[string]string),
	}, nil
}

// Decode deserializes the command payload into the target value.
func (c *Command) Decode(target any) error {
	if len(c.Data) == 0 {
		return nil
	}
	if b, ok := target.(*[]byte); ok {
		*b = append([]byte(nil), c.Data...)
		return nil
	}
	if s, ok := target.(*string); ok {
		*s = string(c.Data)
		return nil
	}
	return json.Unmarshal(c.Data, target)
}

// IdempotencyKey returns the idempotency key if present in metadata.
func (c *Command) IdempotencyKey() string {
	if c.Metadata == nil {
		return ""
	}
	return c.Metadata[MetadataKeyIdempotency]
}

// SetIdempotencyKey sets the idempotency key in metadata.
func (c *Command) SetIdempotencyKey(key string) {
	if c.Metadata == nil {
		c.Metadata = make(map[string]string)
	}
	c.Metadata[MetadataKeyIdempotency] = key
}

// SetMetadata sets a key-value pair in command metadata.
func (c *Command) SetMetadata(key, val string) {
	if c.Metadata == nil {
		c.Metadata = make(map[string]string)
	}
	c.Metadata[key] = val
}

// GetMetadata retrieves a key from command metadata.
func (c *Command) GetMetadata(key string) string {
	if c.Metadata == nil {
		return ""
	}
	return c.Metadata[key]
}

// Marshal encodes the command into binary for Raft log replication.
func (c *Command) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

// UnmarshalCommand decodes binary Raft log entry into a Command.
func UnmarshalCommand(data []byte) (*Command, error) {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, fmt.Errorf("unmarshal command: %w", err)
	}
	return &cmd, nil
}
