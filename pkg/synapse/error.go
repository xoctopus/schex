package synapse

// Error defines error codes in synapse
// +genx:code
type Error uint8

const (
	ERROR_UNDEFINED Error = iota
	ERROR__SYNAPSE_CLOSED
	ERROR__SYNAPSE_CLOSE_TIMEOUT
)
