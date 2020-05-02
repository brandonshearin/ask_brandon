package graphtest

import (
	"time"

	"github.com/brandonshearin/ask_brandon/linkgraph/graph"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
	gc "gopkg.in/check.v1"
)

/*SuiteBase defines a re-usable set of tests that
can be executed against any type that implements graph*/
type SuiteBase struct {
	g graph.Graph
}

func (s *SuiteBase) SetGraph(g graph.Graph) {
	s.g = g
}

//TestFindLink verifies the find link logic
func (s *SuiteBase) TestFindLink(c *gc.C) {
	/*case 1: link not found*/
	link := &graph.Link{
		URL:         "https//:example.com",
		RetrievedAt: time.Now(),
	}
	_, err := s.g.FindLink(link.ID)
	c.Assert(err, gc.NotNil)

	/*add a link to the store, and check to see it is found*/
	s.g.UpsertLink(link)
	storedLink, err := s.g.FindLink(link.ID)
	c.Assert(err, gc.IsNil)
	c.Assert(storedLink, gc.DeepEquals, link, gc.Commentf("expected link to be found by ID"))

}

// TestUpsertLink verifies the link upsert logic.
func (s *SuiteBase) TestUpsertLink(c *gc.C) {
	// Create a new link
	original := &graph.Link{
		URL:         "https://example.com",
		RetrievedAt: time.Now().Add(-10 * time.Hour),
	}

	err := s.g.UpsertLink(original)
	c.Assert(err, gc.IsNil)
	c.Assert(original.ID, gc.Not(gc.Equals), uuid.Nil, gc.Commentf("expected a linkID to be assigned to the new link"))

	// Update existing link with a newer timestamp and different URL
	accessedAt := time.Now().Truncate(time.Second).UTC()
	existing := &graph.Link{
		ID:          original.ID,
		URL:         "https://example.com",
		RetrievedAt: accessedAt,
	}
	err = s.g.UpsertLink(existing)
	c.Assert(err, gc.IsNil)
	c.Assert(existing.ID, gc.Equals, original.ID, gc.Commentf("link ID changed while upserting"))

	stored, err := s.g.FindLink(existing.ID)
	c.Assert(err, gc.IsNil)
	c.Assert(stored.RetrievedAt, gc.Equals, accessedAt, gc.Commentf("last accessed timestamp was not updated"))

	// Attempt to insert a new link whose URL matches an existing link
	// and provide an older accessedAt value
	sameURL := &graph.Link{
		URL:         existing.URL,
		RetrievedAt: time.Now().Add(-10 * time.Hour).UTC(),
	}
	err = s.g.UpsertLink(sameURL)
	c.Assert(err, gc.IsNil)
	c.Assert(sameURL.ID, gc.Equals, existing.ID)

	stored, err = s.g.FindLink(existing.ID)
	c.Assert(err, gc.IsNil)
	c.Assert(stored.RetrievedAt, gc.Equals, accessedAt, gc.Commentf("last accessed timestamp was overwritten with an older value"))

	// Create a new link and then attempt to update its URL to the same as
	// an existing link.
	dup := &graph.Link{
		URL: "foo",
	}
	err = s.g.UpsertLink(dup)
	c.Assert(err, gc.IsNil)
	c.Assert(dup.ID, gc.Not(gc.Equals), uuid.Nil, gc.Commentf("expected a linkID to be assigned to the new link"))
}

func (s *SuiteBase) TestUpsertEdge(c *gc.C) {
	// IDs := []uuid.UUID{uuid.New(), uuid.New()}
	links := []graph.Link{
		{
			// ID:          IDs[0],
			// RetrievedAt: time.Now(),
			URL: "example1.com",
		},
		{
			// ID:          IDs[1],
			// RetrievedAt: time.Now(),
			URL: "example2.com",
		},
	}
	s.g.UpsertLink(&links[0])
	s.g.UpsertLink(&links[1])
	edge := graph.Edge{
		// ID:        uuid.New(),
		Src: links[0].ID,
		Dst: links[1].ID,
		// UpdatedAt: time.Now().Add(-time.Hour * 10),
	}

	/*Create edge:*/
	err := s.g.UpsertEdge(&edge)
	c.Assert(err, gc.IsNil)
	c.Assert(edge.ID, gc.NotNil, gc.Commentf("edge did not get an ID"))
	c.Assert(edge.UpdatedAt, gc.NotNil, gc.Commentf("edge did not get an updatedAt timestamp"))

	/*update edge that already exists between link 1 and 2*/
	newEdge := graph.Edge{
		Src: links[0].ID,
		Dst: links[1].ID,
	}

	err = s.g.UpsertEdge(&newEdge)

	c.Assert(err, gc.IsNil)
	c.Assert(newEdge.ID, gc.Equals, edge.ID, gc.Commentf("new edge should be given same ID of existing edge"))
	c.Assert(newEdge.UpdatedAt, gc.Not(gc.Equals), edge.UpdatedAt, gc.Commentf("new edge should have 'updatedAt' timestamp after existing edge timestamp"))

	//failure case: upsert edge fails when src and dst links don't exist
	bogusEdge := &graph.Edge{
		UpdatedAt: time.Now(),
	}

	err = s.g.UpsertEdge(bogusEdge)
	c.Assert(err, gc.NotNil)
	c.Assert(xerrors.Is(err, graph.ErrUnknownEdgeLinks), gc.Equals, true)

}
