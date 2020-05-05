package indextest

import (
	"time"

	"github.com/brandonshearin/ask_brandon/textindexer/index"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
	gc "gopkg.in/check.v1"
)

/*
SuiteBase defines a re-usable set of index-realted tests that can
be executed against any type that implements index.Indexer (our in memory
bleve index implementaion, and our elasticsearch index)
*/
type SuiteBase struct {
	idx index.Indexer
}

/*
SetIndexer configures the test-suite to run all tests against the idx param.
Exactly like how we set the in memory link graph implementation
*/
func (s *SuiteBase) SetIndexer(idx index.Indexer) {
	s.idx = idx
}

//TestIndexDocument verifies the indexing logic for new documents
func (s *SuiteBase) TestIndexDocument(c *gc.C) {
	incompleteDoc := &index.Document{
		Content:  "Hello example content",
		Title:    "Title 1",
		URL:      "http://example.com",
		PageRank: 1,
	}

	err := s.idx.Index(incompleteDoc)
	c.Assert(err, gc.NotNil)
	c.Assert(xerrors.Is(err, index.ErrMissingLinkID), gc.Equals, true)

	id := uuid.New()
	doc := &index.Document{
		Content:   "Hello example content",
		Title:     "Title 1",
		URL:       "http://example.com",
		PageRank:  1,
		LinkID:    id,
		IndexedAt: time.Now().Add(-12 * time.Hour),
	}

	err = s.idx.Index(doc)
	c.Assert(err, gc.IsNil)
}

//TestFindByID verifies the document lookup logic
func (s *SuiteBase) TestFindByID(c *gc.C) {
	doc := &index.Document{
		LinkID:    uuid.New(),
		Content:   "hello",
		IndexedAt: time.Now(),
		PageRank:  1,
		Title:     "Title",
		URL:       "http://example.com",
	}
	err := s.idx.Index(doc)
	c.Assert(err, gc.IsNil)

	got, err := s.idx.FindByID(doc.LinkID)
	c.Assert(err, gc.IsNil)
	c.Assert(got, gc.DeepEquals, doc, gc.Commentf("document returned from FindByID does not match inserted document"))

	got, err = s.idx.FindByID(uuid.New())
	c.Assert(got, gc.IsNil)
	c.Assert(xerrors.Is(err, index.ErrNotFound), gc.Equals, true)
}

//TestUpdateScore verifies that a document's pagerank is changed correctly
func (s *SuiteBase) TestUpdateScore(c *gc.C) {
	doc := &index.Document{
		LinkID:   uuid.New(),
		PageRank: 1,
	}
	err := s.idx.Index(doc)
	c.Assert(err, gc.IsNil)

	err = s.idx.UpdateScore(doc.LinkID, float64(5))
	c.Assert(err, gc.IsNil)
	got, err := s.idx.FindByID(doc.LinkID)
	c.Assert(err, gc.IsNil)
	c.Assert(got.PageRank, gc.Equals, float64(5), gc.Commentf("PageRank score not updated"))
}

//TestUpdateScoreUnknownDocument verifies that PageRank score is updated on documents that aren't indexed
func (s *SuiteBase) TestUpdateScoreUnknownDocument(c *gc.C) {
	id := uuid.New()
	err := s.idx.UpdateScore(id, float64(10))
	c.Assert(err, gc.IsNil)
	found, err := s.idx.FindByID(id)
	c.Assert(err, gc.IsNil)

	c.Assert(found.URL, gc.Equals, "")
	c.Assert(found.Title, gc.Equals, "")
	c.Assert(found.Content, gc.Equals, "")
	c.Assert(found.PageRank, gc.Equals, float64(10))

}
