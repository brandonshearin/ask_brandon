package graph

import (
	"time"

	"github.com/google/uuid"
)

/*Graph will be used by objects to perform crawler operations*/
type Graph interface {
	UpsertLink(link *Link) error
	FindLink(id uuid.UUID) (*Link, error)

	UpsertEdge(edge *Edge) error
	RemoveStaleEdges(fromID uuid.UUID, updatedBefore time.Time) error

	/*Returns a set of links whose ID is within the (fromID, toID) range. Eventually
	we want to partition links and edges into non-overlapping regions to be processed in parallel */
	Links(fromID, toID uuid.UUID, retrievedBefore time.Time) (LinkIterator, error)
	/*Returns a set of edges that have a Src Link with a UUID within the (fromID, toID) range*/
	Edges(fromID, toID uuid.UUID, updatedBefore time.Time) (EdgeIterator, error)
}

/*Link is a representation of a link object in our graph.  It has a URL and a timestamp for when it was
last retrieved*/
type Link struct {
	ID          uuid.UUID
	URL         string
	RetrievedAt time.Time
}

/*Edge logically represents the connection of links.  The Src uuid is the uuid of
the current link, and any link on that page will be Dst uuid*/
type Edge struct {
	ID        uuid.UUID
	Src       uuid.UUID
	Dst       uuid.UUID
	UpdatedAt time.Time
}

/*LinkIterator is implemented by object that can iterate graph links.  Since there
is no upper bound on number of Links (or Edges) our graph can have, we
want to implement iterator design pattern and lazily fetch Link and Edge models on demand.*/
type LinkIterator interface {
	Iterator
	//Link returns the fetched link object
	Link() *Link
}

/*EdgeIterator is implemnted by objects that can iterate the graph edges*/
type EdgeIterator interface {
	Iterator
	//Edge returns the fetched edge object
	Edge() *Edge
}

/*Iterator is implemented by both LinkIterator and EdgeIterator.  Both
implementations will need these 3 functions*/
type Iterator interface {
	/*Advance the iterator, if no more items or an error occurs
	calls to Next() return false*/
	Next() bool

	/*Error returns the last encountered error by the iterator*/
	Error() error

	/*Release any resources associated with an iterator*/
	Close() error
}
