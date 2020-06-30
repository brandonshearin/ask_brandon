package bspgraph

import "github.com/brandonshearin/ask_brandon/bspgraph/message"

// Aggregator is implemnted by types that provide concurrent-safe
// aggregation primitives (counters, min/max)
type Aggregator interface {
	// Type returns the type of this aggregator
	Type() string

	// Set the aggregator to the specified value
	Set(val interface{})

	// Get the current aggregator value
	Get() interface{}

	// Aggregate updates the aggregator's value based on the provided value
	Aggregate(val interface{})

	// Delta returns the change in the aggregator's value
	// since the last call to Delta
	Delta() interface{}
}

// Relayer is implmented by types that can relay messages to vertices that are managed
// by a remote graph instance
type Relayer interface {
	// Relay a message to a vertex that is not known locally.  Calls to
	// Relay must return ErrDestinationIsLocal if the provided dst value is
	// not a valid remote destination
	Relay(dst string, msg message.Message) error
}

func (g *Graph) RegisterRelayer(relayer Relayer) {
	g.relayer = relayer
}

// The RelayerFunc type is an adapter to allow the use of ordinary functions as
// Relayers.  If f is a function with the appropriate signature,
// RelayerFunc(f) is a Relayer that calls f.
type RelayerFunc func(string, message.Message) error

// Relay calls f(dst, msg)
func (f RelayerFunc) Relay(dst string, msg message.Message) error {
	return f(dst, msg)
}

// ComputeFunc is a function that a graph instance invokes on each vertex when
// executing a superstep.
type ComputeFunc func(g *Graph, v *Vertex, msgIt message.Iterator) error
