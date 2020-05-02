package memory

import "github.com/brandonshearin/ask_brandon/linkgraph/graph"

type edgeIterator struct {
	s            *InMemoryGraph
	list         []*graph.Edge
	currentIndex int
}

func (e *edgeIterator) Close() error {
	return nil
}

func (e *edgeIterator) Error() error {
	return nil
}

func (e *edgeIterator) Edge() *graph.Edge {
	e.s.mu.RLock()
	/*make copy of edge*/
	edgeCopy := new(graph.Edge)
	*edgeCopy = *e.list[e.currentIndex-1]
	e.s.mu.RUnlock()
	return edgeCopy
}

func (e *edgeIterator) Next() bool {
	if e.currentIndex < len(e.list)-1 {
		e.currentIndex++
		return true
	}
	return false
}
