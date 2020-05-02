package memory

import (
	"sync"
	"time"

	"github.com/brandonshearin/ask_brandon/linkgraph/graph"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

type edgeList []uuid.UUID

/*InMemoryGraph implements the Graph interface, in memort, for finding and modifying links*/
type InMemoryGraph struct {
	mu sync.RWMutex

	links map[uuid.UUID]*graph.Link
	edges map[uuid.UUID]*graph.Edge

	linkURLIndex map[string]*graph.Link
	linkEdgeMap  map[uuid.UUID]edgeList
}

/*NewInMemoryGraph initializes an in memory implemntation of our graph */
func NewInMemoryGraph() *InMemoryGraph {
	return &InMemoryGraph{
		links:        make(map[uuid.UUID]*graph.Link),
		edges:        make(map[uuid.UUID]*graph.Edge),
		linkURLIndex: make(map[string]*graph.Link),
		linkEdgeMap:  make(map[uuid.UUID]edgeList),
	}
}

/*UpsertLink allows a caller to update/insert a link into the in memory graph*/
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

/*UpsertEdge allows a caller to update/insert an edge into the in memory graph store*/
func (s *InMemoryGraph) UpsertEdge(edge *graph.Edge) error {
	/*acquire write lock*/
	s.mu.Lock()
	defer s.mu.Unlock()

	/*determine if src and dst links actually exist*/
	_, srcExists := s.links[edge.Src]
	_, dstExists := s.links[edge.Dst]
	if !dstExists || !srcExists {
		return xerrors.Errorf("upsert edge: %w", graph.ErrUnknownEdgeLinks)
	}

	/*scan edges coming out of src link and check for an existing edge
	to same destination*/
	for _, edgeID := range s.linkEdgeMap[edge.Src] {
		existingEdge := s.edges[edgeID]
		/*Update timestamp if match found*/
		if existingEdge.Dst == edge.Dst && existingEdge.Src == edge.Src {
			existingEdge.UpdatedAt = time.Now()
			*edge = *existingEdge
			return nil
		}
	}

	/*If preceding loop does not produce a match, create and insert a new edge
	to the in memory store.*/
	for {
		edge.ID = uuid.New()
		if s.edges[edge.ID] == nil {
			break
		}
	}

	/*create a copy of the provided edge object to insert into the store*/
	edge.UpdatedAt = time.Now()
	edgeCopy := new(graph.Edge)
	*edgeCopy = *edge
	s.edges[edgeCopy.ID] = edgeCopy

	/*add edge to list of edges originating from the edge's source link*/
	s.linkEdgeMap[edge.Src] = append(s.linkEdgeMap[edge.Src], edgeCopy.ID)
	return nil
}

/*FindLink allows a caller to fetch a link by uuid from the in memory graph*/
func (s *InMemoryGraph) FindLink(id uuid.UUID) (*graph.Link, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	link := s.links[id]
	if link == nil {
		return nil, xerrors.Errorf("find link: %w", graph.ErrNotFound)
	}

	/*return a copy of the link... This ensures that no external code can
	modify the actual link in our graph.*/
	linkCopy := new(graph.Link)
	*linkCopy = *link
	return linkCopy, nil
}

/*Links returns an iterator that can iterate over a specified partition of link objects in our in memory store*/
func (s *InMemoryGraph) Links(fromID, toID uuid.UUID, retrievedBefore time.Time) (graph.LinkIterator, error) {
	from, to := fromID.String(), toID.String()

	s.mu.RLock()
	var list []*graph.Link
	/*Partition links in the graph*/
	for uuid, link := range s.links {
		if uuid.String() >= from && uuid.String() < to && link.RetrievedAt.Before(retrievedBefore) {
			list = append(list, link)
		}
	}
	s.mu.RUnlock()

	return &linkIterator{
		s:     s,
		links: list,
	}, nil
}

/*Edges returns an iterator with edges belonging to a partition based on the edge's src link*/
func (s *InMemoryGraph) Edges(fromID, toID uuid.UUID, updatedBefore time.Time) (graph.EdgeIterator, error) {
	from, to := fromID.String(), toID.String()

	s.mu.RLock()
	var list []*graph.Edge
	/*iterate through links in the fromID, toID range.
	Edges are partitioned based on the link they originate from*/
	for linkID := range s.links {
		if id := linkID.String(); id >= from && id < to {
			continue
		}
		/*scan through edges originating from link*/
		for _, edgeID := range s.linkEdgeMap[linkID] {
			/*append edge to list if it satisfies time constraint*/
			if edge := s.edges[edgeID]; edge.UpdatedAt.Before(updatedBefore) {
				list = append(list, edge)
			}
		}
	}
	s.mu.RUnlock()

	return &edgeIterator{s: s, list: list}, nil
}

/*RemoveStaleEdges traverses the edge list from a source link, removing edges
that were last updated before the 'updatedBefore' param.*/
func (s *InMemoryGraph) RemoveStaleEdges(fromID uuid.UUID, updatedBefore time.Time) error {
	/*As with the other operations that mutate the graph's contents,
	i.e. UpsertLink() and UpsertEdge(), we need to acquire a write lock*/
	s.mu.Lock()
	defer s.mu.Unlock()

	var newEdgeList edgeList
	for _, edgeID := range s.linkEdgeMap[fromID] {
		if edge := s.edges[edgeID]; edge.UpdatedAt.Before(updatedBefore) {
			/*remove stale edge from in-memory edges map*/
			delete(s.edges, edgeID)
			continue
		}
		newEdgeList = append(newEdgeList, edgeID)
	}
	s.linkEdgeMap[fromID] = newEdgeList
	return nil
}
