package memory

import "github.com/brandonshearin/ask_brandon/linkgraph/graph"

type linkIterator struct {
	links []*graph.Link
	s     *InMemoryGraph
	//keep track of iterators current offset within the links slice
	currentIndex int
}

func (l *linkIterator) Close() error {
	return nil
}

func (l *linkIterator) Error() error {
	return nil
}

func (l *linkIterator) Next() bool {
	if l.currentIndex < len(l.links)-1 {
		l.currentIndex++
		return true
	}
	return false
}

func (l *linkIterator) Link() *graph.Link {
	/*because the link objects associated with the iterator are maintained by the in-memory store, multiple
	iterators may be acting on these link objects.  As a result, one iterator may be consuming links and another
	may be mutating them.  To avoid data races, we obtain a read lock on the link graph so that we can safely
	read a link and return a copy to the caller  */
	l.s.mu.RLock()
	linkCopy := new(graph.Link)
	*linkCopy = *l.links[l.currentIndex-1]
	l.s.mu.RUnlock()
	return linkCopy
}
