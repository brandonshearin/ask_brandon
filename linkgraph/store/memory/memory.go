package memory

import (
	"sync"
	"time"

	"github.com/brandonshearin/ask_brandon/linkgraph/graph"
	"github.com/google/uuid"
)

type edgeList []uuid.UUID

type InMemoryGraph struct {
	mu sync.RWMutex

	links map[uuid.UUID]*graph.Link
	edges map[uuid.UUID]*graph.Edge

	linkURLIndex map[string]*graph.Link
	linkEdgeMap  map[uuid.UUID]edgeList
}

func NewInMemoryGraph() *InMemoryGraph {
	return &InMemoryGraph{
		links:        make(map[uuid.UUID]*graph.Link),
		edges:        make(map[uuid.UUID]*graph.Edge),
		linkURLIndex: make(map[string]*graph.Link),
		linkEdgeMap:  make(map[uuid.UUID]edgeList),
	}
}
func (s *InMemoryGraph) UpsertLink(link *graph.Link) error {
	/*Since an upsert operation always modifies the graph, method should
	acquire a write lock.*/
	s.mu.Lock()
	defer s.mu.Unlock()

	/*if link with the same URL already exists in the in-memory graph,
	we need to update it in-place with new timestamp.*/
	if existing := s.linkURLIndex[link.URL]; existing != nil {
		//grab the existing ID for re-use
		link.ID = existing.ID
		//grab the existing timestamp
		originalTimestamp := existing.RetrievedAt
		//replace existing link obj with the new link obj...
		//this is the magic.  Updating the pointer to the existing link
		//also updates other data structures that have a pointer to the
		//old link, such as the links map[].  The entry in the links map
		//for uuid 1234, for example, now points to a link with same uuid
		//of 1234, but an updated timestamp
		*existing = *link
		//ensure that the the most recent 'RetrievedAt' time is saved
		if originalTimestamp.After(existing.RetrievedAt) {
			existing.RetrievedAt = originalTimestamp
		}
		return nil
	}

	/*Once we verify that we need to insert instead of update,
	we need to find a uuid that doesn't exist in our graph yet.
	In the highly unlikely case of a UUID collision:*/
	for {
		link.ID = uuid.New()
		if s.links[link.ID] == nil {
			break
		}
	}

	linkCopy := new(graph.Link)
	*linkCopy = *link
	s.linkURLIndex[linkCopy.URL] = linkCopy
	s.links[linkCopy.ID] = linkCopy
	return nil
}
func (s *InMemoryGraph) FindLink(id uuid.UUID) (*graph.Link, error)                       { return nil, nil }
func (s *InMemoryGraph) UpsertEdge(edge *graph.Edge) error                                { return nil }
func (s *InMemoryGraph) RemoveStaleEdges(fromID uuid.UUID, updatedBefore time.Time) error { return nil }
func (s *InMemoryGraph) Links(fromID, toID uuid.UUID, retrievedBefore time.Time) (graph.LinkIterator, error) {
	return nil, nil
}
func (s *InMemoryGraph) Edges(fromID, toID uuid.UUID, updatedBefore time.Time) (graph.EdgeIterator, error) {
	return nil, nil
}
