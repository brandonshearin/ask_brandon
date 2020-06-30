package shortestpath

import (
	"github.com/brandonshearin/ask_brandon/bspgraph"
	"github.com/brandonshearin/ask_brandon/bspgraph/message"
)

// PathCostMessage is used to broadcasy the cost of a path through a vertex
type PathCostMessage struct {
	// The ID of the vertex this cost announcement originates from.
	FromID string

	// The cost of the path form this vertex to the source vertex via FromID
	Cost int
}

func (pc PathCostMessage) Type() string { return "cost" }

// each vertex maintains its own pathState instance, which is stored a sthe vertex
// value
type pathState struct {
	minDist    int
	prevInPath string
}

func (c *Calculator) findShortestPath(g *bspgraph.Graph, v *bspgraph.Vertex, msgIt message.Iterator) error {

}
