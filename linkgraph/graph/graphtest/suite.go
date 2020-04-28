package graphtest

import (
	"time"

	"github.com/brandonshearin/ask_brandon/linkgraph/graph"
	"github.com/google/uuid"
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

	// stored, err := s.g.FindLink(existing.ID)
	// c.Assert(err, gc.IsNil)
	// c.Assert(stored.RetrievedAt, gc.Equals, accessedAt, gc.Commentf("last accessed timestamp was not updated"))

	// Attempt to insert a new link whose URL matches an existing link with
	// and provide an older accessedAt value
	sameURL := &graph.Link{
		URL:         existing.URL,
		RetrievedAt: time.Now().Add(-10 * time.Hour).UTC(),
	}
	err = s.g.UpsertLink(sameURL)
	c.Assert(err, gc.IsNil)
	c.Assert(sameURL.ID, gc.Equals, existing.ID)

	// stored, err = s.g.FindLink(existing.ID)
	// c.Assert(err, gc.IsNil)
	// c.Assert(stored.RetrievedAt, gc.Equals, accessedAt, gc.Commentf("last accessed timestamp was overwritten with an older value"))

	// Create a new link and then attempt to update its URL to the same as
	// an existing link.
	dup := &graph.Link{
		URL: "foo",
	}
	err = s.g.UpsertLink(dup)
	c.Assert(err, gc.IsNil)
	c.Assert(dup.ID, gc.Not(gc.Equals), uuid.Nil, gc.Commentf("expected a linkID to be assigned to the new link"))
}
